package nsqd

import (
	"bytes"
	"container/heap"
	"errors"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nsqio/nsq/internal/pqueue"
	"github.com/nsqio/nsq/internal/quantile"
)

type Consumer interface {
	UnPause()
	Pause()
	Close() error
	TimedOutMessage()
	Stats() ClientStats
	Empty()
}

// Channel represents the concrete type for a NSQ channel (and also
// implements the Queue interface)
//
// There can be multiple channels per topic, each with there own unique set
// of subscribers (clients).
//
// Channels maintain all client and message metadata, orchestrating in-flight
// messages, timeouts, requeuing, etc.
type Channel struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	requeueCount uint64
	messageCount uint64
	timeoutCount uint64

	sync.RWMutex

	topicName string
	name      string
	ctx       *context

	backend BackendQueue

	memoryMsgChan chan *Message
	exitFlag      int32
	exitMutex     sync.RWMutex

	// state tracking
	clients        map[int64]Consumer
	paused         int32
	ephemeral      bool
	deleteCallback func(*Channel)
	deleter        sync.Once

	// Stats tracking
	e2eProcessingLatencyStream *quantile.Quantile

	// TODO: these can be DRYd up
	deferredMessages map[MessageID]*pqueue.Item
	deferredPQ       pqueue.PriorityQueue
	deferredMutex    sync.Mutex
	inFlightMessages map[MessageID]*Message
	inFlightPQ       inFlightPqueue
	inFlightMutex    sync.Mutex
}

// NewChannel creates a new instance of the Channel type and returns a pointer
// 创建channel
func NewChannel(topicName string, channelName string, ctx *context,
	deleteCallback func(*Channel)) *Channel {

	// 声明初始化channel数据结构
	c := &Channel{
		topicName:      topicName,
		name:           channelName,
		memoryMsgChan:  make(chan *Message, ctx.nsqd.getOpts().MemQueueSize),
		clients:        make(map[int64]Consumer),
		deleteCallback: deleteCallback,
		ctx:            ctx,
	}
	// e2eProcessingLatencyStream主要用于统计消息投递的延迟等
	if len(ctx.nsqd.getOpts().E2EProcessingLatencyPercentiles) > 0 {
		c.e2eProcessingLatencyStream = quantile.New(
			ctx.nsqd.getOpts().E2EProcessingLatencyWindowTime,
			ctx.nsqd.getOpts().E2EProcessingLatencyPercentiles,
		)
	}

	// initPQ函数创建了两个字典inFlightMessages、deferredMessages和两个队列inFlightPQ、deferredPQ
	// 在nsq中inFlight指的是正在投递但还没确认投递成功的消息，defferred指的是投递失败，等待重新投递的消息
	// initPQ创建的字典和队列主要用于索引和存放这两类消息
	c.initPQ()

	// 如果是临时channel则backend使用newDummyBackendQueue，如果不是则使用newDiskQueue
	if strings.HasSuffix(channelName, "#ephemeral") {
		c.ephemeral = true
		c.backend = newDummyBackendQueue()
	} else {
		// backend names, for uniqueness, automatically include the topic...
		backendName := getBackendName(topicName, channelName)
		c.backend = newDiskQueue(backendName,
			ctx.nsqd.getOpts().DataPath,
			ctx.nsqd.getOpts().MaxBytesPerFile,
			int32(minValidMsgLength),
			int32(ctx.nsqd.getOpts().MaxMsgSize)+minValidMsgLength,
			ctx.nsqd.getOpts().SyncEvery,
			ctx.nsqd.getOpts().SyncTimeout,
			ctx.nsqd.getOpts().Logger)
	}

	// 通知topic有channel变化需要更新
	c.ctx.nsqd.Notify(c)

	return c
}

// initPQ函数创建了两个字典inFlightMessages、deferredMessages和两个队列inFlightPQ、deferredPQ
// 在nsq中inFlight指的是正在投递但还没确认投递成功的消息，defferred指的是投递失败，等待重新投递的消息
// initPQ创建的字典和队列主要用于索引和存放这两类消息
func (c *Channel) initPQ() {
	pqSize := int(math.Max(1, float64(c.ctx.nsqd.getOpts().MemQueueSize)/10))

	c.inFlightMessages = make(map[MessageID]*Message)
	c.deferredMessages = make(map[MessageID]*pqueue.Item)

	c.inFlightMutex.Lock()
	c.inFlightPQ = newInFlightPqueue(pqSize)
	c.inFlightMutex.Unlock()

	c.deferredMutex.Lock()
	c.deferredPQ = pqueue.New(pqSize)
	c.deferredMutex.Unlock()
}

// Exiting returns a boolean indicating if this channel is closed/exiting
// 判断channel是否正在退出中
func (c *Channel) Exiting() bool {
	return atomic.LoadInt32(&c.exitFlag) == 1
}

// Delete empties the channel and closes
// 删除channel
func (c *Channel) Delete() error {
	return c.exit(true)
}

// Close cleanly closes the Channel
// 关闭channel
func (c *Channel) Close() error {
	return c.exit(false)
}

// 退出channel的统一接口
func (c *Channel) exit(deleted bool) error {
	c.exitMutex.Lock()
	defer c.exitMutex.Unlock()

	if !atomic.CompareAndSwapInt32(&c.exitFlag, 0, 1) {
		return errors.New("exiting")
	}

	// 如果是删除channel则通知topic有channel更新
	if deleted {
		c.ctx.nsqd.logf("CHANNEL(%s): deleting", c.name)

		// since we are explicitly deleting a channel (not just at system exit time)
		// de-register this from the lookupd
		c.ctx.nsqd.Notify(c)
	} else {
		c.ctx.nsqd.logf("CHANNEL(%s): closing", c.name)
	}

	// this forceably closes client connections
	// 关闭所有的客户端消费者
	c.RLock()
	for _, client := range c.clients {
		client.Close()
	}
	c.RUnlock()

	// 如果是删除则需要清空channel里面的消息，同时将backend删除
	if deleted {
		// empty the queue (deletes the backend files, too)
		c.Empty()
		return c.backend.Delete()
	}

	// write anything leftover to disk
	c.flush()
	return c.backend.Close()
}

// 清空channel的消息
func (c *Channel) Empty() error {
	c.Lock()
	defer c.Unlock()

	// 初始化channel，让延时和正在投递的消息丢弃掉
	c.initPQ()
	// 让所有的消费者清空掉消息
	for _, client := range c.clients {
		client.Empty()
	}

	// 将内存通道中的消息清空掉
	for {
		select {
		case <-c.memoryMsgChan:
		default:
			goto finish
		}
	}

finish:
	// 将backend清空掉
	return c.backend.Empty()
}

// flush persists all the messages in internal memory buffers to the backend
// it does not drain inflight/deferred because it is only called in Close()
// 将内存中的消息写入到磁盘中
func (c *Channel) flush() error {
	var msgBuf bytes.Buffer

	if len(c.memoryMsgChan) > 0 || len(c.inFlightMessages) > 0 || len(c.deferredMessages) > 0 {
		c.ctx.nsqd.logf("CHANNEL(%s): flushing %d memory %d in-flight %d deferred messages to backend",
			c.name, len(c.memoryMsgChan), len(c.inFlightMessages), len(c.deferredMessages))
	}

	for {
		select {
		// 将内存通道中的消息写入到磁盘中
		case msg := <-c.memoryMsgChan:
			err := writeMessageToBackend(&msgBuf, msg, c.backend)
			if err != nil {
				c.ctx.nsqd.logf("ERROR: failed to write message to backend - %s", err)
			}
		default:
			goto finish
		}
	}

finish:
	// 将正在投递的消息写入到磁盘中
	for _, msg := range c.inFlightMessages {
		err := writeMessageToBackend(&msgBuf, msg, c.backend)
		if err != nil {
			c.ctx.nsqd.logf("ERROR: failed to write message to backend - %s", err)
		}
	}

	// 将延时的消息写入到磁盘中
	for _, item := range c.deferredMessages {
		msg := item.Value.(*Message)
		err := writeMessageToBackend(&msgBuf, msg, c.backend)
		if err != nil {
			c.ctx.nsqd.logf("ERROR: failed to write message to backend - %s", err)
		}
	}

	return nil
}

func (c *Channel) Depth() int64 {
	return int64(len(c.memoryMsgChan)) + c.backend.Depth()
}

// 暂停channel
func (c *Channel) Pause() error {
	return c.doPause(true)
}

// 取消暂停channel
func (c *Channel) UnPause() error {
	return c.doPause(false)
}

// 暂停和取消暂停的统一接口
func (c *Channel) doPause(pause bool) error {
	if pause {
		atomic.StoreInt32(&c.paused, 1)
	} else {
		atomic.StoreInt32(&c.paused, 0)
	}

	c.RLock()
	// 让所有的消费者暂停或取消暂停
	for _, client := range c.clients {
		if pause {
			client.Pause()
		} else {
			client.UnPause()
		}
	}
	c.RUnlock()
	return nil
}

// 判断channel是否暂停
func (c *Channel) IsPaused() bool {
	return atomic.LoadInt32(&c.paused) == 1
}

// PutMessage writes a Message to the queue
// 向channel写入消息
func (c *Channel) PutMessage(m *Message) error {
	c.RLock()
	defer c.RUnlock()
	if atomic.LoadInt32(&c.exitFlag) == 1 {
		return errors.New("exiting")
	}
	err := c.put(m)
	if err != nil {
		return err
	}
	atomic.AddUint64(&c.messageCount, 1)
	return nil
}

// 底层统一向channel投递消息的接口
func (c *Channel) put(m *Message) error {
	select {
	// 如果内存通道还没有满，则直接投递到内存通道中
	case c.memoryMsgChan <- m:
	// 这种情况就是内存通道已满，则需要就爱那个消息写入到磁盘中
	default:
		// 获得一个缓冲
		b := bufferPoolGet()
		// 将消息写入到磁盘中
		err := writeMessageToBackend(b, m, c.backend)
		// 释放缓冲
		bufferPoolPut(b)
		c.ctx.nsqd.SetHealth(err)
		if err != nil {
			c.ctx.nsqd.logf("CHANNEL(%s) ERROR: failed to write message to backend - %s",
				c.name, err)
			return err
		}
	}
	return nil
}

// TouchMessage resets the timeout for an in-flight message
// 消费者发送TOUCH，表明该消息的超时值需要被重置
// 从inFlightPQ中取出消息，设置新的超时值后重新放入队列，新的超时值由当前时间、
// 客户端通过IDENTIFY设置的超时值、配置中允许的最大超时值MaxMsgTimeout共同决定
func (c *Channel) TouchMessage(clientID int64, id MessageID, clientMsgTimeout time.Duration) error {
	// 从正在投递的map里面取出消息
	msg, err := c.popInFlightMessage(clientID, id)
	if err != nil {
		return err
	}
	// 将消息从投递队列里面删除掉
	c.removeFromInFlightPQ(msg)

	// 当前时间加上客户端提供的过期时间得到最新的过期时间点
	newTimeout := time.Now().Add(clientMsgTimeout)
	// 设置的过期时间不能超过配置里面的最大过期时间,如果超过了则设置过期时间为配置信息里的最大的过期时间
	if newTimeout.Sub(msg.deliveryTS) >=
		c.ctx.nsqd.getOpts().MaxMsgTimeout {
		// we would have gone over, set to the max
		newTimeout = msg.deliveryTS.Add(c.ctx.nsqd.getOpts().MaxMsgTimeout)
	}

	// 重新设置消息过期时间
	msg.pri = newTimeout.UnixNano()
	// 然后将消息重新放入投递map中
	err = c.pushInFlightMessage(msg)
	if err != nil {
		return err
	}
	// 将消息重新投递到队列里
	c.addToInFlightPQ(msg)
	return nil
}

// FinishMessage successfully discards an in-flight message
// 消费者发送FIN，表明消息已经被接收并正确处理
func (c *Channel) FinishMessage(clientID int64, id MessageID) error {
	// 从正在投递的map里面取出消息
	msg, err := c.popInFlightMessage(clientID, id)
	if err != nil {
		return err
	}
	// 从正在投递的优先级消息队列里面删除消息
	c.removeFromInFlightPQ(msg)
	if c.e2eProcessingLatencyStream != nil {
		c.e2eProcessingLatencyStream.Insert(msg.Timestamp)
	}
	return nil
}

// RequeueMessage requeues a message based on `time.Duration`, ie:
//
// `timeoutMs` == 0 - requeue a message immediately
// `timeoutMs`  > 0 - asynchronously wait for the specified timeout
//     and requeue a message (aka "deferred requeue")
//
// 对消息投递失败进行处理。该函数将消息从inFlightMessages和inFlightPQ中删除， 随后进行重新投递
func (c *Channel) RequeueMessage(clientID int64, id MessageID, timeout time.Duration) error {
	// remove from inflight first
	// 从正在投递的map里面取出消息
	msg, err := c.popInFlightMessage(clientID, id)
	if err != nil {
		return err
	}
	// 将该消息从投递中的优先级队列中删除
	c.removeFromInFlightPQ(msg)

	// 如果重新投递的超时时间是0，则表示立刻进行投递
	if timeout == 0 {
		c.exitMutex.RLock()
		err := c.doRequeue(msg)
		c.exitMutex.RUnlock()
		return err
	}

	// deferred requeue
	// 否则将该消息加入到延迟投递的优先级队列中
	return c.StartDeferredTimeout(msg, timeout)
}

// AddClient adds a client to the Channel's client list
// 向channel增加消费者
func (c *Channel) AddClient(clientID int64, client Consumer) {
	c.Lock()
	defer c.Unlock()

	_, ok := c.clients[clientID]
	if ok {
		return
	}
	c.clients[clientID] = client
}

// RemoveClient removes a client from the Channel's client list
// 将channel中的消费者删除
func (c *Channel) RemoveClient(clientID int64) {
	c.Lock()
	defer c.Unlock()

	_, ok := c.clients[clientID]
	if !ok {
		return
	}
	delete(c.clients, clientID)

	if len(c.clients) == 0 && c.ephemeral == true {
		go c.deleter.Do(func() { c.deleteCallback(c) })
	}
}

// 填充消息的消费者ID、投送时间、优先级，然后调用pushInFlightMessage函数将消息放入inFlightMessages字典中
func (c *Channel) StartInFlightTimeout(msg *Message, clientID int64, timeout time.Duration) error {
	now := time.Now()
	msg.clientID = clientID
	msg.deliveryTS = now
	msg.pri = now.Add(timeout).UnixNano()
	// 将消息添加到发送中的优先级中
	err := c.pushInFlightMessage(msg)
	if err != nil {
		return err
	}
	// 加入到发送中的map中
	c.addToInFlightPQ(msg)
	return nil
}

// 填充消息的消费者ID、投送时间、优先级，然后调用pushDeferredMessage函数将消息放入deferredMessages字典中
func (c *Channel) StartDeferredTimeout(msg *Message, timeout time.Duration) error {
	absTs := time.Now().Add(timeout).UnixNano()
	item := &pqueue.Item{Value: msg, Priority: absTs}
	// 添加到延迟优先级队列里
	err := c.pushDeferredMessage(item)
	if err != nil {
		return err
	}
	// 加入到延迟消息map中
	c.addToDeferredPQ(item)
	return nil
}

// doRequeue performs the low level operations to requeue a message
//
// Callers of this method need to ensure that a simultaneous exit will not occur
// 将消息重新投递到channel中
func (c *Channel) doRequeue(m *Message) error {
	err := c.put(m)
	if err != nil {
		return err
	}
	// 将重新投递次数加一
	atomic.AddUint64(&c.requeueCount, 1)
	return nil
}

// pushInFlightMessage atomically adds a message to the in-flight dictionary
// 将消息添加到正在投递中的map中
func (c *Channel) pushInFlightMessage(msg *Message) error {
	c.inFlightMutex.Lock()
	_, ok := c.inFlightMessages[msg.ID]
	if ok {
		c.inFlightMutex.Unlock()
		return errors.New("ID already in flight")
	}
	c.inFlightMessages[msg.ID] = msg
	c.inFlightMutex.Unlock()
	return nil
}

// popInFlightMessage atomically removes a message from the in-flight dictionary
// 将消息从正在发送中的消息map中移除掉
func (c *Channel) popInFlightMessage(clientID int64, id MessageID) (*Message, error) {
	c.inFlightMutex.Lock()
	msg, ok := c.inFlightMessages[id]
	if !ok {
		c.inFlightMutex.Unlock()
		return nil, errors.New("ID not in flight")
	}
	if msg.clientID != clientID {
		c.inFlightMutex.Unlock()
		return nil, errors.New("client does not own message")
	}
	delete(c.inFlightMessages, id)
	c.inFlightMutex.Unlock()
	return msg, nil
}

// 将消息添加到正在发送中的优先级队列里面
func (c *Channel) addToInFlightPQ(msg *Message) {
	c.inFlightMutex.Lock()
	c.inFlightPQ.Push(msg)
	c.inFlightMutex.Unlock()
}

// 将消息从正在发送中的优先级队列里面删除
func (c *Channel) removeFromInFlightPQ(msg *Message) {
	c.inFlightMutex.Lock()
	if msg.index == -1 {
		// this item has already been popped off the pqueue
		c.inFlightMutex.Unlock()
		return
	}
	c.inFlightPQ.Remove(msg.index)
	c.inFlightMutex.Unlock()
}

// 将消息添加到延迟发送的消息map中
func (c *Channel) pushDeferredMessage(item *pqueue.Item) error {
	c.deferredMutex.Lock()
	// TODO: these map lookups are costly
	id := item.Value.(*Message).ID
	_, ok := c.deferredMessages[id]
	if ok {
		c.deferredMutex.Unlock()
		return errors.New("ID already deferred")
	}
	c.deferredMessages[id] = item
	c.deferredMutex.Unlock()
	return nil
}

// 将消息从延迟发送的消息map中删除掉
func (c *Channel) popDeferredMessage(id MessageID) (*pqueue.Item, error) {
	c.deferredMutex.Lock()
	// TODO: these map lookups are costly
	item, ok := c.deferredMessages[id]
	if !ok {
		c.deferredMutex.Unlock()
		return nil, errors.New("ID not deferred")
	}
	delete(c.deferredMessages, id)
	c.deferredMutex.Unlock()
	return item, nil
}

// 将消息添加到延迟发送优先级队列里
func (c *Channel) addToDeferredPQ(item *pqueue.Item) {
	c.deferredMutex.Lock()
	heap.Push(&c.deferredPQ, item)
	c.deferredMutex.Unlock()
}

// 取出deferredPQ顶部的消息，如果当前消息已经超时，则将消息从队列中移除，并返回消息
// 由于队列是优先级队列，所以如果popDeferredMessage取出的消息为空，则不需要再往后取了
//直接返回false表示当前非dirty状态。 如果取到了消息，则说明该消息投递超时，需要把消息传入doRequeue立即重新投递
func (c *Channel) processDeferredQueue(t int64) bool {
	c.exitMutex.RLock()
	defer c.exitMutex.RUnlock()

	if c.Exiting() {
		return false
	}

	dirty := false
	for {
		c.deferredMutex.Lock()
		item, _ := c.deferredPQ.PeekAndShift(t)
		c.deferredMutex.Unlock()

		if item == nil {
			goto exit
		}
		dirty = true

		msg := item.Value.(*Message)
		_, err := c.popDeferredMessage(msg.ID)
		if err != nil {
			goto exit
		}
		// 将消息重新投递到channel里面
		c.doRequeue(msg)
	}

exit:
	return dirty
}

// 取出inFlightPQ顶部的消息，如果当前消息已经超时，则将消息从队列中移除，并返回消息
// 由于队列是优先级队列，所以如果processInFlightQueue取出的消息为空，则不需要再往后取了
//直接返回false表示当前非dirty状态。 如果取到了消息，则说明该消息投递超时，需要把消息传入doRequeue立即重新投递
func (c *Channel) processInFlightQueue(t int64) bool {
	c.exitMutex.RLock()
	defer c.exitMutex.RUnlock()

	if c.Exiting() {
		return false
	}

	dirty := false
	for {
		c.inFlightMutex.Lock()
		msg, _ := c.inFlightPQ.PeekAndShift(t)
		c.inFlightMutex.Unlock()

		if msg == nil {
			goto exit
		}
		dirty = true

		_, err := c.popInFlightMessage(msg.clientID, msg.ID)
		if err != nil {
			goto exit
		}
		atomic.AddUint64(&c.timeoutCount, 1)
		c.RLock()
		// 得到消费者
		client, ok := c.clients[msg.clientID]
		c.RUnlock()
		if ok {
			client.TimedOutMessage()
		}
		// 将超时的消息重新投递到channel中
		c.doRequeue(msg)
	}

exit:
	return dirty
}
