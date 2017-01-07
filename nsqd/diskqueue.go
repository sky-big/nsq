package nsqd

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"
)

// diskQueue implements the BackendQueue interface
// providing a filesystem backed FIFO queue
type diskQueue struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms

	// run-time state (also persisted to disk)
	readPos      int64
	writePos     int64
	readFileNum  int64
	writeFileNum int64
	depth        int64

	sync.RWMutex

	// instantiation time metadata
	name            string
	dataPath        string
	maxBytesPerFile int64 // currently this cannot change once created
	minMsgSize      int32
	maxMsgSize      int32
	syncEvery       int64         // number of writes per fsync
	syncTimeout     time.Duration // duration of time per fsync
	exitFlag        int32
	needSync        bool

	// keeps track of the position where we have read
	// (but not yet sent over readChan)
	nextReadPos     int64
	nextReadFileNum int64

	readFile  *os.File
	writeFile *os.File
	reader    *bufio.Reader
	writeBuf  bytes.Buffer

	// exposed via ReadChan()
	readChan chan []byte

	// internal channels
	writeChan         chan []byte
	writeResponseChan chan error
	emptyChan         chan int
	emptyResponseChan chan error
	exitChan          chan int
	exitSyncChan      chan int

	logger Logger
}

// newDiskQueue instantiates a new instance of diskQueue, retrieving metadata
// from the filesystem and starting the read ahead goroutine
// 声明初始化diskqueue数据结构
func newDiskQueue(name string, dataPath string, maxBytesPerFile int64,
	minMsgSize int32, maxMsgSize int32,
	syncEvery int64, syncTimeout time.Duration,
	logger Logger) BackendQueue {
	// 初始化一个diskqueue数据结构
	d := diskQueue{
		name:              name,
		dataPath:          dataPath,
		maxBytesPerFile:   maxBytesPerFile,
		minMsgSize:        minMsgSize,
		maxMsgSize:        maxMsgSize,
		readChan:          make(chan []byte),
		writeChan:         make(chan []byte),
		writeResponseChan: make(chan error),
		emptyChan:         make(chan int),
		emptyResponseChan: make(chan error),
		exitChan:          make(chan int),
		exitSyncChan:      make(chan int),
		syncEvery:         syncEvery,
		syncTimeout:       syncTimeout,
		logger:            logger,
	}

	// no need to lock here, nothing else could possibly be touching this instance
	// 通过retrieveMetaData函数获取之前与该diskQueue相关联的Topic/Channel已经持久化的信息
	err := d.retrieveMetaData()
	if err != nil && !os.IsNotExist(err) {
		d.logf("ERROR: diskqueue(%s) failed to retrieveMetaData - %s", d.name, err)
	}

	go d.ioLoop()

	return &d
}

// backend日志打印函数
func (d *diskQueue) logf(f string, args ...interface{}) {
	if d.logger == nil {
		return
	}
	d.logger.Output(2, fmt.Sprintf(f, args...))
}

// Depth returns the depth of the queue
func (d *diskQueue) Depth() int64 {
	return atomic.LoadInt64(&d.depth)
}

// ReadChan returns the []byte channel for reading data
// 返回本backend的读取消息的通道
func (d *diskQueue) ReadChan() chan []byte {
	return d.readChan
}

// Put writes a []byte to the queue
// 向backend写数据,该函数比较简单，加锁，并且将数据放入d.writeChan，等待d.writeResponseChan的结果后返回
func (d *diskQueue) Put(data []byte) error {
	d.RLock()
	defer d.RUnlock()

	if d.exitFlag == 1 {
		return errors.New("exiting")
	}

	d.writeChan <- data
	return <-d.writeResponseChan
}

// Close cleans up the queue and persists metadata
// 关闭backend
func (d *diskQueue) Close() error {
	err := d.exit(false)
	if err != nil {
		return err
	}
	// 同步状态信息到状态文件中
	return d.sync()
}

// 删除backend
func (d *diskQueue) Delete() error {
	return d.exit(true)
}

// backend的退出统一函数
func (d *diskQueue) exit(deleted bool) error {
	d.Lock()
	defer d.Unlock()

	d.exitFlag = 1

	if deleted {
		d.logf("DISKQUEUE(%s): deleting", d.name)
	} else {
		d.logf("DISKQUEUE(%s): closing", d.name)
	}

	close(d.exitChan)
	// ensure that ioLoop has exited
	<-d.exitSyncChan

	// 将读取的磁盘文件关闭
	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}

	// 将写的磁盘文件关闭
	if d.writeFile != nil {
		d.writeFile.Close()
		d.writeFile = nil
	}

	return nil
}

// Empty destructively clears out any pending data in the queue
// by fast forwarding read positions and removing intermediate files
// 清空backend
func (d *diskQueue) Empty() error {
	d.RLock()
	defer d.RUnlock()

	if d.exitFlag == 1 {
		return errors.New("exiting")
	}

	d.logf("DISKQUEUE(%s): emptying", d.name)

	d.emptyChan <- 1
	return <-d.emptyResponseChan
}

// 删除所有的磁盘文件
func (d *diskQueue) deleteAllFiles() error {
	// 将当前backend的所有磁盘文件删除掉
	err := d.skipToNextRWFile()

	// 将backend的状态文件删除掉
	innerErr := os.Remove(d.metaDataFileName())
	if innerErr != nil && !os.IsNotExist(innerErr) {
		d.logf("ERROR: diskqueue(%s) failed to remove metadata file - %s", d.name, innerErr)
		return innerErr
	}

	return err
}

// 将当前backend的所有磁盘文件删除掉
func (d *diskQueue) skipToNextRWFile() error {
	var err error

	// 将当前正在读取的磁盘句柄关闭
	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}

	// 将当前正在写的磁盘句柄关闭
	if d.writeFile != nil {
		d.writeFile.Close()
		d.writeFile = nil
	}

	// 从读文件开始到写磁盘文件，将所有的磁盘文件全部删除掉
	for i := d.readFileNum; i <= d.writeFileNum; i++ {
		fn := d.fileName(i)
		innerErr := os.Remove(fn)
		if innerErr != nil && !os.IsNotExist(innerErr) {
			d.logf("ERROR: diskqueue(%s) failed to remove data file - %s", d.name, innerErr)
			err = innerErr
		}
	}

	// 将所有的读写文件名字和偏移位置同步到下一个空的磁盘文件
	d.writeFileNum++
	d.writePos = 0
	d.readFileNum = d.writeFileNum
	d.readPos = 0
	d.nextReadFileNum = d.writeFileNum
	d.nextReadPos = 0
	atomic.StoreInt64(&d.depth, 0)

	return err
}

// readOne performs a low level filesystem read for a single []byte
// while advancing read positions and rolling files, if necessary
// backend从内部读取数据(调用一次即读取一条消息)
func (d *diskQueue) readOne() ([]byte, error) {
	var err error
	var msgSize int32

	// 如果读取的句柄不存在，则打开读取句柄
	if d.readFile == nil {
		curFileName := d.fileName(d.readFileNum)
		d.readFile, err = os.OpenFile(curFileName, os.O_RDONLY, 0600)
		if err != nil {
			return nil, err
		}

		d.logf("DISKQUEUE(%s): readOne() opened %s", d.name, curFileName)

		// 将读取的位置定位到磁盘文件中
		if d.readPos > 0 {
			_, err = d.readFile.Seek(d.readPos, 0)
			if err != nil {
				d.readFile.Close()
				d.readFile = nil
				return nil, err
			}
		}

		d.reader = bufio.NewReader(d.readFile)
	}

	// 将四个字节的消息长度先读取出来
	err = binary.Read(d.reader, binary.BigEndian, &msgSize)
	if err != nil {
		d.readFile.Close()
		d.readFile = nil
		return nil, err
	}

	// 验证消息长度的合法性
	if msgSize < d.minMsgSize || msgSize > d.maxMsgSize {
		// this file is corrupt and we have no reasonable guarantee on
		// where a new message should begin
		d.readFile.Close()
		d.readFile = nil
		return nil, fmt.Errorf("invalid message read size (%d)", msgSize)
	}

	// 根据消息的长度将消息读取出来
	readBuf := make([]byte, msgSize)
	_, err = io.ReadFull(d.reader, readBuf)
	if err != nil {
		d.readFile.Close()
		d.readFile = nil
		return nil, err
	}

	totalBytes := int64(4 + msgSize)

	// we only advance next* because we have not yet sent this to consumers
	// (where readFileNum, readPos will actually be advanced)
	// 更新文件的最新读取偏移位置
	d.nextReadPos = d.readPos + totalBytes
	d.nextReadFileNum = d.readFileNum

	// TODO: each data file should embed the maxBytesPerFile
	// as the first 8 bytes (at creation time) ensuring that
	// the value can change without affecting runtime
	// 如果偏移位置超过了文件的最大大小，则将当前磁盘文件关闭，更新下一个要读取的文件以及文件偏移位置
	if d.nextReadPos > d.maxBytesPerFile {
		if d.readFile != nil {
			d.readFile.Close()
			d.readFile = nil
		}

		d.nextReadFileNum++
		d.nextReadPos = 0
	}

	return readBuf, nil
}

// writeOne performs a low level filesystem write for a single []byte
// while advancing write positions and rolling files, if necessary
// backend内部写消息的函数
func (d *diskQueue) writeOne(data []byte) error {
	var err error

	// 若当前要写的文件不存在，则通过d.fileName(d.writeFileNum)获得文件名，并创建文件
	if d.writeFile == nil {
		curFileName := d.fileName(d.writeFileNum)
		d.writeFile, err = os.OpenFile(curFileName, os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			return err
		}

		d.logf("DISKQUEUE(%s): writeOne() opened %s", d.name, curFileName)

		// 将当前写的位置定位到文件中
		if d.writePos > 0 {
			_, err = d.writeFile.Seek(d.writePos, 0)
			if err != nil {
				d.writeFile.Close()
				d.writeFile = nil
				return err
			}
		}
	}

	// 得到要写入数据的长度
	dataLen := int32(len(data))

	// 判断数据的长度的合法性
	if dataLen < d.minMsgSize || dataLen > d.maxMsgSize {
		return fmt.Errorf("invalid message write size (%d) maxMsgSize=%d", dataLen, d.maxMsgSize)
	}

	// 先将四位的缓冲数据长度写入到缓冲头部
	d.writeBuf.Reset()
	err = binary.Write(&d.writeBuf, binary.BigEndian, dataLen)
	if err != nil {
		return err
	}

	// 然后继续将数据写入缓冲
	_, err = d.writeBuf.Write(data)
	if err != nil {
		return err
	}

	// only write to the file once
	// 然后将缓冲中的数据写入到磁盘文件中
	_, err = d.writeFile.Write(d.writeBuf.Bytes())
	if err != nil {
		d.writeFile.Close()
		d.writeFile = nil
		return err
	}

	// 更新最新的写的位置
	totalBytes := int64(4 + dataLen)
	d.writePos += totalBytes
	atomic.AddInt64(&d.depth, 1)

	// 单个磁盘文件的长度不能超过单个文件的最大数量，如果超过了则需要新创建一个磁盘文件
	if d.writePos > d.maxBytesPerFile {
		// 磁盘文件名字加一
		d.writeFileNum++
		// 写的位置置0
		d.writePos = 0

		// sync every time we start writing to a new file
		// 将当前磁盘文件同步到磁盘，然后将当前backend的状态信息写入到状态文件中
		err = d.sync()
		if err != nil {
			d.logf("ERROR: diskqueue(%s) failed to sync - %s", d.name, err)
		}

		// 将老的磁盘文件关闭掉
		if d.writeFile != nil {
			d.writeFile.Close()
			d.writeFile = nil
		}
	}

	return err
}

// sync fsyncs the current writeFile and persists metadata
// 将当前磁盘文件同步到磁盘，然后将当前backend的状态信息写入到状态文件中
func (d *diskQueue) sync() error {
	// 将当前磁盘文件同步到磁盘
	if d.writeFile != nil {
		err := d.writeFile.Sync()
		if err != nil {
			d.writeFile.Close()
			d.writeFile = nil
			return err
		}
	}

	// 然后将当前backend的状态信息写入到状态文件中
	err := d.persistMetaData()
	if err != nil {
		return err
	}

	d.needSync = false
	return nil
}

// retrieveMetaData initializes state from the filesystem
// retrieveMetaData函数从磁盘中恢复diskQueue的状态
// diskQueue会定时将自己的状态备份到文件中， 文件名由metaDataFileName函数确定
// retrieveMetaData函数同样通过metaDataFileName函数获得保存状态的文件名并打开
// 该文件只有三行，格式为%d\n%d,%d\n%d,%d\n，第一行保存着该diskQueue中消息的数量（depth），
// 第二行保存readFileNum和readPos，第三行保存writeFileNum和writePos
func (d *diskQueue) retrieveMetaData() error {
	var f *os.File
	var err error

	// 得到状态文件的名字
	fileName := d.metaDataFileName()
	// 打开状态文件
	f, err = os.OpenFile(fileName, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	var depth int64
	// 将状态文件中的信息读取出来
	_, err = fmt.Fscanf(f, "%d\n%d,%d\n%d,%d\n",
		&depth,
		&d.readFileNum, &d.readPos,
		&d.writeFileNum, &d.writePos)
	if err != nil {
		return err
	}
	atomic.StoreInt64(&d.depth, depth)
	d.nextReadFileNum = d.readFileNum
	d.nextReadPos = d.readPos

	return nil
}

// persistMetaData atomically writes state to the filesystem
// 将运行时的元数据保存到文件用于下次重新构建diskQueue时的恢复
func (d *diskQueue) persistMetaData() error {
	var f *os.File
	var err error

	// 得到状态文件的名字
	fileName := d.metaDataFileName()
	// 生成一个临时文件名字
	tmpFileName := fmt.Sprintf("%s.%d.tmp", fileName, rand.Int())

	// write to tmp file
	// 打开临时文件
	f, err = os.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}

	// 将状态信息写入到临时文件中
	_, err = fmt.Fprintf(f, "%d\n%d,%d\n%d,%d\n",
		atomic.LoadInt64(&d.depth),
		d.readFileNum, d.readPos,
		d.writeFileNum, d.writePos)
	if err != nil {
		f.Close()
		return err
	}
	f.Sync()
	f.Close()

	// atomically rename
	// 将临时状态文件更名为正式的状态文件名字
	return atomicRename(tmpFileName, fileName)
}

// 得到状态文件名字
func (d *diskQueue) metaDataFileName() string {
	return fmt.Sprintf(path.Join(d.dataPath, "%s.diskqueue.meta.dat"), d.name)
}

// 得到存储数据的磁盘文件名字
func (d *diskQueue) fileName(fileNum int64) string {
	return fmt.Sprintf(path.Join(d.dataPath, "%s.diskqueue.%06d.dat"), d.name, fileNum)
}

func (d *diskQueue) checkTailCorruption(depth int64) {
	if d.readFileNum < d.writeFileNum || d.readPos < d.writePos {
		return
	}

	// we've reached the end of the diskqueue
	// if depth isn't 0 something went wrong
	if depth != 0 {
		if depth < 0 {
			d.logf(
				"ERROR: diskqueue(%s) negative depth at tail (%d), metadata corruption, resetting 0...",
				d.name, depth)
		} else if depth > 0 {
			d.logf(
				"ERROR: diskqueue(%s) positive depth at tail (%d), data loss, resetting 0...",
				d.name, depth)
		}
		// force set depth 0
		atomic.StoreInt64(&d.depth, 0)
		d.needSync = true
	}

	if d.readFileNum != d.writeFileNum || d.readPos != d.writePos {
		if d.readFileNum > d.writeFileNum {
			d.logf(
				"ERROR: diskqueue(%s) readFileNum > writeFileNum (%d > %d), corruption, skipping to next writeFileNum and resetting 0...",
				d.name, d.readFileNum, d.writeFileNum)
		}

		if d.readPos > d.writePos {
			d.logf(
				"ERROR: diskqueue(%s) readPos > writePos (%d > %d), corruption, skipping to next writeFileNum and resetting 0...",
				d.name, d.readPos, d.writePos)
		}

		d.skipToNextRWFile()
		d.needSync = true
	}
}

// 当消息投递成功后，则使用moveForward函数将保存在d.nextReadPos和d.nextReadFileNum中的值取出，
// 赋值给d.readPos和d.readFileNum，moveForward函数还负责清理已经读完的旧文件
func (d *diskQueue) moveForward() {
	// 更新当前读取的文件以及读取的偏移位置
	oldReadFileNum := d.readFileNum
	d.readFileNum = d.nextReadFileNum
	d.readPos = d.nextReadPos
	depth := atomic.AddInt64(&d.depth, -1)

	// see if we need to clean up the old file
	// 如果老的读取文件和现在要读取的文件名字不同，则表明上次读取的文件已经读完消息，上次的磁盘文件已经无用，需要删除掉
	if oldReadFileNum != d.nextReadFileNum {
		// sync every time we start reading from a new file
		d.needSync = true

		// 将老的磁盘文件删除
		fn := d.fileName(oldReadFileNum)
		err := os.Remove(fn)
		if err != nil {
			d.logf("ERROR: failed to Remove(%s) - %s", fn, err)
		}
	}

	d.checkTailCorruption(depth)
}

// 处理读取一个消息失败的函数
func (d *diskQueue) handleReadError() {
	// jump to the next read file and rename the current (bad) file
	// 如果当前写的文件跟读的文件是一样的，则将写的文件关闭掉，从下一个文件开始写入
	if d.readFileNum == d.writeFileNum {
		// if you can't properly read from the current write file it's safe to
		// assume that something is fucked and we should skip the current file too
		if d.writeFile != nil {
			d.writeFile.Close()
			d.writeFile = nil
		}
		d.writeFileNum++
		d.writePos = 0
	}

	// 得到读取失败的文件名字
	badFn := d.fileName(d.readFileNum)
	badRenameFn := badFn + ".bad"

	d.logf(
		"NOTICE: diskqueue(%s) jump to next file and saving bad file as %s",
		d.name, badRenameFn)

	// 重命名
	err := atomicRename(badFn, badRenameFn)
	if err != nil {
		d.logf(
			"ERROR: diskqueue(%s) failed to rename bad diskqueue file %s to %s",
			d.name, badFn, badRenameFn)
	}

	// 读取文件也进入下一个读取文件进行读取,偏移位置置0
	d.readFileNum++
	d.readPos = 0
	d.nextReadFileNum = d.readFileNum
	d.nextReadPos = 0

	// significant state change, schedule a sync on the next iteration
	d.needSync = true
}

// ioLoop provides the backend for exposing a go channel (via ReadChan())
// in support of multiple concurrent queue consumers
//
// it works by looping and branching based on whether or not the queue has data
// to read and blocking until data is either read or written over the appropriate
// go channels
//
// conveniently this also means that we're asynchronously reading from the filesystem
// ioLoop函数实现了diskQueue的消息循环，diskQueue的定时操作和读写操作的核心都在这个函数中完成
func (d *diskQueue) ioLoop() {
	var dataRead []byte
	var err error
	var count int64
	var r chan []byte

	// 首先使用time.NewTicker(d.syncTimeout)定义了syncTicker变量，syncTicker的类型是time.Ticker，
	// 每隔d.syncTimeout时间就会在syncTicker.C这个go channel产生一个消息
	syncTicker := time.NewTicker(d.syncTimeout)

	for {
		// dont sync all the time :)
		// ioLoop的定时任务是调用sync函数刷新文件，防止突然结束程序后内存中的内容未被提交到磁盘，导致内容丢失
		// 控制是否需要同步的变量是d.needSync，该变量在一次sync后会被置为false，在许多需要刷新文件的地方会被置为true
		if count == d.syncEvery {
			d.needSync = true
		}

		// 如果needSync变化为true，则需要进行一次将内存中的消息同步到磁盘文件中
		if d.needSync {
			err = d.sync()
			if err != nil {
				d.logf("ERROR: diskqueue(%s) failed to sync - %s", d.name, err)
			}
			// 同步一次之后将count数量重置为0
			count = 0
		}

		// 检测当前是否有数据需要被读取，如果(d.readFileNum < d.writeFileNum) || (d.readPos < d.writePos) 和`d.nextReadPos == d.readPos这两个条件成立，
		// 则执行d.readOne()并将结果放入dataRead中，然后设置r为d.readChan,如果条件不成立，则将r置为空值nil
		if (d.readFileNum < d.writeFileNum) || (d.readPos < d.writePos) {
			if d.nextReadPos == d.readPos {
				dataRead, err = d.readOne()
				if err != nil {
					d.logf("ERROR: reading from diskqueue(%s) at %d of %s - %s",
						d.name, d.readPos, d.fileName(d.readFileNum), err)
					d.handleReadError()
					continue
				}
			}
			r = d.readChan
		} else {
			r = nil
		}

		select {
		// the Go channel spec dictates that nil channel operations (read or write)
		// in a select are skipped, we set r to d.readChan only when there is data to read
		// 将读到的数据发送到读通道，如果r通道为nil则会直接跳过这个分之
		case r <- dataRead:
			count++
			// moveForward sets needSync flag if a file is removed
			d.moveForward()
		// 如果通道收到信息，则立刻删除所有的磁盘数据
		case <-d.emptyChan:
			d.emptyResponseChan <- d.deleteAllFiles()
			count = 0
		// 写通道收到数据进行写数据
		case dataWrite := <-d.writeChan:
			count++
			d.writeResponseChan <- d.writeOne(dataWrite)
		// 定时器收到信息需要进行同步刷新
		case <-syncTicker.C:
			if count == 0 {
				// avoid sync when there's no activity
				continue
			}
			d.needSync = true
		// 手动退出信号
		case <-d.exitChan:
			goto exit
		}
	}

exit:
	d.logf("DISKQUEUE(%s): closing ... ioLoop", d.name)
	syncTicker.Stop()
	d.exitSyncChan <- 1
}
