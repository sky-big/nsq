package nsqd

import (
	"bytes"
	"errors"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/nsqio/nsq/internal/quantile"
	"github.com/nsqio/nsq/internal/util"
)

type Topic struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	messageCount uint64

	sync.RWMutex

	name              string
	channelMap        map[string]*Channel
	backend           BackendQueue
	memoryMsgChan     chan *Message
	exitChan          chan int
	channelUpdateChan chan int
	waitGroup         util.WaitGroupWrapper
	exitFlag          int32

	ephemeral      bool
	deleteCallback func(*Topic)
	deleter        sync.Once

	paused    int32
	pauseChan chan bool

	ctx *context
}

// Topic constructor
// 创建新的topic信息
func NewTopic(topicName string, ctx *context, deleteCallback func(*Topic)) *Topic {
	// 声明初始化一个topic数据结构
	t := &Topic{
		name:              topicName,
		channelMap:        make(map[string]*Channel),
		memoryMsgChan:     make(chan *Message, ctx.nsqd.getOpts().MemQueueSize),
		exitChan:          make(chan int),
		channelUpdateChan: make(chan int),
		ctx:               ctx,
		pauseChan:         make(chan bool),
		deleteCallback:    deleteCallback,
	}

	// 如果是临时topic，则backend启动newDummyBackendQueue类型，该类型接收到消息后什么都不做，直接将消息丢弃掉
	// 如果不是临时topic，则backend启动newDiskQueue
	if strings.HasSuffix(topicName, "#ephemeral") {
		t.ephemeral = true
		t.backend = newDummyBackendQueue()
	} else {
		t.backend = newDiskQueue(topicName,
			ctx.nsqd.getOpts().DataPath,
			ctx.nsqd.getOpts().MaxBytesPerFile,
			int32(minValidMsgLength),
			int32(ctx.nsqd.getOpts().MaxMsgSize)+minValidMsgLength,
			ctx.nsqd.getOpts().SyncEvery,
			ctx.nsqd.getOpts().SyncTimeout,
			ctx.nsqd.getOpts().Logger)
	}

	// 开启一个新的goroutine来执行messagePump函数，该函数负责消息循环，将进入topic中的消息投递到channel中
	t.waitGroup.Wrap(func() { t.messagePump() })

	// 该函数在topic和channel创建、停止的时候调用， Notify函数通过执行PersistMetadata函数，将topic和channel的信息写到文件中
	t.ctx.nsqd.Notify(t)

	return t
}

// Exiting returns a boolean indicating if this topic is closed/exiting
// 判断当前topic是否在退出中
func (t *Topic) Exiting() bool {
	return atomic.LoadInt32(&t.exitFlag) == 1
}

// GetChannel performs a thread safe operation
// to return a pointer to a Channel object (potentially new)
// for the given Topic
// 根据channel名字得到对应的channel信息指针,如果对应的channel不存在则创建新的channel
// 同事通知topic协程该topic下的channel信息有更新
func (t *Topic) GetChannel(channelName string) *Channel {
	t.Lock()
	channel, isNew := t.getOrCreateChannel(channelName)
	t.Unlock()

	if isNew {
		// update messagePump state
		select {
		// 通知topicchannel信息有更新
		case t.channelUpdateChan <- 1:
		case <-t.exitChan:
		}
	}

	return channel
}

// this expects the caller to handle locking
// 根据channel名字得到对应的channel信息指针,如果对应的channel不存在则创建新的channel
func (t *Topic) getOrCreateChannel(channelName string) (*Channel, bool) {
	channel, ok := t.channelMap[channelName]
	// 如果不存在对应的channel，则创建新的channel
	if !ok {
		deleteCallback := func(c *Channel) {
			t.DeleteExistingChannel(c.name)
		}
		channel = NewChannel(t.name, channelName, t.ctx, deleteCallback)
		t.channelMap[channelName] = channel
		t.ctx.nsqd.logf("TOPIC(%s): new channel(%s)", t.name, channel.name)
		return channel, true
	}
	return channel, false
}

// 根据channel名字得到存在的channel，如果不存在则返回空
func (t *Topic) GetExistingChannel(channelName string) (*Channel, error) {
	t.RLock()
	defer t.RUnlock()
	channel, ok := t.channelMap[channelName]
	if !ok {
		return nil, errors.New("channel does not exist")
	}
	return channel, nil
}

// DeleteExistingChannel removes a channel from the topic only if it exists
// 根据channel名字删除存在的channel
func (t *Topic) DeleteExistingChannel(channelName string) error {
	t.Lock()
	channel, ok := t.channelMap[channelName]
	if !ok {
		t.Unlock()
		return errors.New("channel does not exist")
	}
	delete(t.channelMap, channelName)
	// not defered so that we can continue while the channel async closes
	numChannels := len(t.channelMap)
	t.Unlock()

	t.ctx.nsqd.logf("TOPIC(%s): deleting channel %s", t.name, channel.name)

	// delete empties the channel before closing
	// (so that we dont leave any messages around)
	channel.Delete()

	// update messagePump state
	// 通知对应的topic有channel信息更新
	select {
	case t.channelUpdateChan <- 1:
	case <-t.exitChan:
	}

	// 如果channel的个数为0，同时是临时的channel，则调用nsqd服务的毁掉删除topic函数
	if numChannels == 0 && t.ephemeral == true {
		go t.deleter.Do(func() { t.deleteCallback(t) })
	}

	return nil
}

// PutMessage writes a Message to the queue
// 客户端通过nsqd的HTTP API或TCP API向特定topic发送消息，nsqd的HTTP或TCP模块通过调用对应topic的PutMessage函数
func (t *Topic) PutMessage(m *Message) error {
	t.RLock()
	defer t.RUnlock()
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return errors.New("exiting")
	}
	err := t.put(m)
	if err != nil {
		return err
	}
	atomic.AddUint64(&t.messageCount, 1)
	return nil
}

// PutMessages writes multiple Messages to the queue
// 客户端通过nsqd的HTTP API或TCP API向特定topic发送消息，nsqd的HTTP或TCP模块通过调用对应topic的PutMessages函数
func (t *Topic) PutMessages(msgs []*Message) error {
	t.RLock()
	defer t.RUnlock()
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return errors.New("exiting")
	}
	for _, m := range msgs {
		err := t.put(m)
		if err != nil {
			return err
		}
	}
	atomic.AddUint64(&t.messageCount, uint64(len(msgs)))
	return nil
}

// 向topic发送消息的最底层接口
func (t *Topic) put(m *Message) error {
	select {
	// 如果通道里面的缓冲还没填满，则继续将消息投递到内存通道里
	case t.memoryMsgChan <- m:
	// 执行到此处表明通道里已经满，则将消息写入到磁盘文件中
	default:
		// 通过bufferPoolGet函数从buffer池中获取一个buffer，bufferPoolGet及以下bufferPoolPut函数是对sync.Pool的简单包装
		b := bufferPoolGet()
		// 向backend里面写入消息，即将消息写入到磁盘
		err := writeMessageToBackend(b, m, t.backend)
		// 将缓冲buffer回收
		bufferPoolPut(b)
		// 调用SetHealth函数将writeMessageToBackend的返回值写入errValue变量。
		// 该变量衍生出IsHealthy，GetError和GetHealth3个函数，主要用于测试以及从HTTP API获取nsqd的运行情况（是否发生错误）
		t.ctx.nsqd.SetHealth(err)
		if err != nil {
			t.ctx.nsqd.logf(
				"TOPIC(%s) ERROR: failed to write message to backend - %s",
				t.name, err)
			return err
		}
	}
	return nil
}

func (t *Topic) Depth() int64 {
	return int64(len(t.memoryMsgChan)) + t.backend.Depth()
}

// messagePump selects over the in-memory and backend queue and
// writes messages to every channel for this topic
// 负责Topic的消息循环，该函数在创建新的topic时通过waitGroup在新的goroutine中运行
func (t *Topic) messagePump() {
	var msg *Message
	var buf []byte
	var err error
	var chans []*Channel
	var memoryMsgChan chan *Message
	var backendChan chan []byte

	// 初始化时先获取当前存在的channel数组，设置memoryMsgChan和backendChan
	t.RLock()
	for _, c := range t.channelMap {
		chans = append(chans, c)
	}
	t.RUnlock()

	if len(chans) > 0 {
		memoryMsgChan = t.memoryMsgChan
		backendChan = t.backend.ReadChan()
	}

	for {
		select {
		// 从客户端发送上来的消息通道里面取得消息
		case msg = <-memoryMsgChan:
		// 处理从backend里面读取出来的消息,不过读取出来的是[]byte,需要转换程message指针
		case buf = <-backendChan:
			msg, err = decodeMessage(buf)
			if err != nil {
				t.ctx.nsqd.logf("ERROR: failed to decode message - %s", err)
				continue
			}
		// 处理当前topic下channel的更新
		case <-t.channelUpdateChan:
			// 从channelMap中取出每个channel，构成channel的数组以便后续进行消息的投递。
			// 并且根据当前是否有channel以及该topic是否处于暂停状态来决定memoryMsgChan和backendChan是否为空
			chans = chans[:0]
			t.RLock()
			for _, c := range t.channelMap {
				chans = append(chans, c)
			}
			t.RUnlock()
			// 如果channel的数量为0或者当前topic为暂停都将停止向channel发送消息
			if len(chans) == 0 || t.IsPaused() {
				memoryMsgChan = nil
				backendChan = nil
			} else {
				memoryMsgChan = t.memoryMsgChan
				backendChan = t.backend.ReadChan()
			}
			continue
		// 处理topic的暂停
		case pause := <-t.pauseChan:
			if pause || len(chans) == 0 {
				memoryMsgChan = nil
				backendChan = nil
			} else {
				memoryMsgChan = t.memoryMsgChan
				backendChan = t.backend.ReadChan()
			}
			continue
		// 处理topic的退出
		case <-t.exitChan:
			goto exit
		}

		// 随后是将消息投到每个channel中，首先先对消息进行复制操作，这里有个优化，对于第一次循环， 直接使用原消息进行发送以减少复制对象的开销
		// ，此后的循环将对消息进行复制。对于即时的消息， 直接调用channel的PutMessage函数进行投递，对于延迟的消息，
		//调用channel的StartDeferredTimeout函数进行投递
		for i, channel := range chans {
			chanMsg := msg
			// copy the message because each channel
			// needs a unique instance but...
			// fastpath to avoid copy if its the first channel
			// (the topic already created the first copy)
			if i > 0 {
				chanMsg = NewMessage(msg.ID, msg.Body)
				chanMsg.Timestamp = msg.Timestamp
				chanMsg.deferred = msg.deferred
			}
			if chanMsg.deferred != 0 {
				channel.StartDeferredTimeout(chanMsg, chanMsg.deferred)
				continue
			}
			err := channel.PutMessage(chanMsg)
			if err != nil {
				t.ctx.nsqd.logf(
					"TOPIC(%s) ERROR: failed to put msg(%s) to channel(%s) - %s",
					t.name, msg.ID, channel.name, err)
			}
		}
	}

exit:
	t.ctx.nsqd.logf("TOPIC(%s): closing ... messagePump", t.name)
}

// Delete empties the topic and all its channels and closes
// 删除topic自己的接口
func (t *Topic) Delete() error {
	return t.exit(true)
}

// Close persists all outstanding topic data and closes all its channels
// 关闭topic的接口
func (t *Topic) Close() error {
	return t.exit(false)
}

// topic退出的接口，deleted为true表示将自己删除掉，否则不会删除
func (t *Topic) exit(deleted bool) error {
	if !atomic.CompareAndSwapInt32(&t.exitFlag, 0, 1) {
		return errors.New("exiting")
	}

	if deleted {
		t.ctx.nsqd.logf("TOPIC(%s): deleting", t.name)

		// since we are explicitly deleting a topic (not just at system exit time)
		// de-register this from the lookupd
		t.ctx.nsqd.Notify(t)
	} else {
		t.ctx.nsqd.logf("TOPIC(%s): closing", t.name)
	}

	close(t.exitChan)

	// synchronize the close of messagePump()
	// 同步等待该topic组里的所有协程关闭
	t.waitGroup.Wait()

	if deleted {
		t.Lock()
		// 如果要删除的话，则需要将当前topic下的所有channel删除掉
		for _, channel := range t.channelMap {
			delete(t.channelMap, channel.name)
			channel.Delete()
		}
		t.Unlock()

		// empty the queue (deletes the backend files, too)
		// 清空topic的消息，先将内存通道里面的消息清空，然后将backend里面的消息也清空
		t.Empty()
		// 如果是持久化topic则将backend也删除
		return t.backend.Delete()
	}

	// close all the channels
	// 执行到此处表明是关闭topic，因此此处是循环将该topic下的所有channel关闭
	for _, channel := range t.channelMap {
		err := channel.Close()
		if err != nil {
			// we need to continue regardless of error to close all the channels
			t.ctx.nsqd.logf("ERROR: channel(%s) close - %s", channel.name, err)
		}
	}

	// write anything leftover to disk
	// 将内存通道里面的消息写入到磁盘中
	t.flush()
	return t.backend.Close()
}

// 清空topic的消息，先将内存通道里面的消息清空，然后将backend里面的消息也清空
func (t *Topic) Empty() error {
	for {
		select {
		case <-t.memoryMsgChan:
		default:
			goto finish
		}
	}

finish:
	return t.backend.Empty()
}

// 将内存通道里面的消息写入到磁盘中
func (t *Topic) flush() error {
	var msgBuf bytes.Buffer

	if len(t.memoryMsgChan) > 0 {
		t.ctx.nsqd.logf(
			"TOPIC(%s): flushing %d memory messages to backend",
			t.name, len(t.memoryMsgChan))
	}

	for {
		select {
		case msg := <-t.memoryMsgChan:
			err := writeMessageToBackend(&msgBuf, msg, t.backend)
			if err != nil {
				t.ctx.nsqd.logf(
					"ERROR: failed to write message to backend - %s", err)
			}
		default:
			goto finish
		}
	}

finish:
	return nil
}

func (t *Topic) AggregateChannelE2eProcessingLatency() *quantile.Quantile {
	var latencyStream *quantile.Quantile
	t.RLock()
	realChannels := make([]*Channel, 0, len(t.channelMap))
	for _, c := range t.channelMap {
		realChannels = append(realChannels, c)
	}
	t.RUnlock()
	for _, c := range realChannels {
		if c.e2eProcessingLatencyStream == nil {
			continue
		}
		if latencyStream == nil {
			latencyStream = quantile.New(
				t.ctx.nsqd.getOpts().E2EProcessingLatencyWindowTime,
				t.ctx.nsqd.getOpts().E2EProcessingLatencyPercentiles)
		}
		latencyStream.Merge(c.e2eProcessingLatencyStream)
	}
	return latencyStream
}

func (t *Topic) Pause() error {
	return t.doPause(true)
}

func (t *Topic) UnPause() error {
	return t.doPause(false)
}

// 暂停topic
func (t *Topic) doPause(pause bool) error {
	if pause {
		atomic.StoreInt32(&t.paused, 1)
	} else {
		atomic.StoreInt32(&t.paused, 0)
	}

	select {
	case t.pauseChan <- pause:
	case <-t.exitChan:
	}

	return nil
}

// 判断topic是否是暂停
func (t *Topic) IsPaused() bool {
	return atomic.LoadInt32(&t.paused) == 1
}
