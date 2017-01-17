package nsqd

import (
	"bytes"
	"encoding/json"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/nsqio/nsq/internal/version"
)

// 重新连接指定lookup服务的回调
func connectCallback(n *NSQD, hostname string, syncTopicChan chan *lookupPeer) func(*lookupPeer) {
	return func(lp *lookupPeer) {
		ci := make(map[string]interface{})
		ci["version"] = version.Binary
		ci["tcp_port"] = n.RealTCPAddr().Port
		ci["http_port"] = n.RealHTTPAddr().Port
		ci["hostname"] = hostname
		ci["broadcast_address"] = n.getOpts().BroadcastAddress

		cmd, err := nsq.Identify(ci)
		if err != nil {
			lp.Close()
			return
		}
		resp, err := lp.Command(cmd)
		if err != nil {
			n.logf("LOOKUPD(%s): ERROR %s - %s", lp, cmd, err)
		} else if bytes.Equal(resp, []byte("E_INVALID")) {
			n.logf("LOOKUPD(%s): lookupd returned %s", lp, resp)
		} else {
			err = json.Unmarshal(resp, &lp.Info)
			if err != nil {
				n.logf("LOOKUPD(%s): ERROR parsing response - %s", lp, resp)
			} else {
				n.logf("LOOKUPD(%s): peer info %+v", lp, lp.Info)
			}
		}

		go func() {
			syncTopicChan <- lp
		}()
	}
}

// 循环向lookup服务发送当前topic，channel的增加删除信息
func (n *NSQD) lookupLoop() {
	var lookupPeers []*lookupPeer
	var lookupAddrs []string
	syncTopicChan := make(chan *lookupPeer)
	connect := true

	hostname, err := os.Hostname()
	if err != nil {
		n.logf("FATAL: failed to get hostname - %s", err)
		os.Exit(1)
	}

	// for announcements, lookupd determines the host automatically
	ticker := time.Tick(15 * time.Second)
	for {
		// 如果connect为true则表明重新向所有的lookup服务进行重新连接
		if connect {
			for _, host := range n.getOpts().NSQLookupdTCPAddresses {
				if in(host, lookupAddrs) {
					continue
				}
				n.logf("LOOKUP(%s): adding peer", host)
				lookupPeer := newLookupPeer(host, n.getOpts().MaxBodySize, n.getOpts().Logger,
					connectCallback(n, hostname, syncTopicChan))
				// 通过发送空的消息来进行对lookup服务器节点的连接
				lookupPeer.Command(nil) // start the connection
				lookupPeers = append(lookupPeers, lookupPeer)
				lookupAddrs = append(lookupAddrs, host)
			}
			n.lookupPeers.Store(lookupPeers)
			connect = false
		}

		select {
		// 定时器到时间遍历所有的lookup连接，遍历所有的lookup连接將心跳包發送到所有的lookup鏈接
		case <-ticker:
			// send a heartbeat and read a response (read detects closed conns)
			for _, lookupPeer := range lookupPeers {
				n.logf("LOOKUPD(%s): sending heartbeat", lookupPeer)
				cmd := nsq.Ping()
				_, err := lookupPeer.Command(cmd)
				if err != nil {
					n.logf("LOOKUPD(%s): ERROR %s - %s", lookupPeer, cmd, err)
				}
			}
		// 接收nsqd中通知topic，channel有变化的通道信息
		case val := <-n.notifyChan:
			var cmd *nsq.Command
			var branch string

			switch val.(type) {
			case *Channel:
				// notify all nsqlookupds that a new channel exists, or that it's removed
				branch = "channel"
				channel := val.(*Channel)
				// 组装channel创建成功的消息
				if channel.Exiting() == true {
					cmd = nsq.UnRegister(channel.topicName, channel.name)
				} else {
					cmd = nsq.Register(channel.topicName, channel.name)
				}
			case *Topic:
				// notify all nsqlookupds that a new topic exists, or that it's removed
				branch = "topic"
				topic := val.(*Topic)
				// 组装topic创建成功的消息
				if topic.Exiting() == true {
					cmd = nsq.UnRegister(topic.name, "")
				} else {
					cmd = nsq.Register(topic.name, "")
				}
			}

			// 通知所有的lookup服务topic，channel创建的消息
			for _, lookupPeer := range lookupPeers {
				n.logf("LOOKUPD(%s): %s %s", lookupPeer, branch, cmd)
				_, err := lookupPeer.Command(cmd)
				if err != nil {
					n.logf("LOOKUPD(%s): ERROR %s - %s", lookupPeer, cmd, err)
				}
			}
		// 向指定的lookup同步本nsqd服务的所有topic，channel信息
		case lookupPeer := <-syncTopicChan:
			var commands []*nsq.Command
			// build all the commands first so we exit the lock(s) as fast as possible
			n.RLock()
			for _, topic := range n.topicMap {
				topic.RLock()
				if len(topic.channelMap) == 0 {
					commands = append(commands, nsq.Register(topic.name, ""))
				} else {
					for _, channel := range topic.channelMap {
						commands = append(commands, nsq.Register(channel.topicName, channel.name))
					}
				}
				topic.RUnlock()
			}
			n.RUnlock()

			for _, cmd := range commands {
				n.logf("LOOKUPD(%s): %s", lookupPeer, cmd)
				_, err := lookupPeer.Command(cmd)
				if err != nil {
					n.logf("LOOKUPD(%s): ERROR %s - %s", lookupPeer, cmd, err)
					break
				}
			}
		// lookup服务有变化，通知重新连接lookup
		case <-n.optsNotificationChan:
			var tmpPeers []*lookupPeer
			var tmpAddrs []string
			for _, lp := range lookupPeers {
				if in(lp.addr, n.getOpts().NSQLookupdTCPAddresses) {
					tmpPeers = append(tmpPeers, lp)
					tmpAddrs = append(tmpAddrs, lp.addr)
					continue
				}
				n.logf("LOOKUP(%s): removing peer", lp)
				lp.Close()
			}
			lookupPeers = tmpPeers
			lookupAddrs = tmpAddrs
			connect = true
		case <-n.exitChan:
			goto exit
		}
	}

exit:
	n.logf("LOOKUP: closing")
}

func in(s string, lst []string) bool {
	for _, v := range lst {
		if s == v {
			return true
		}
	}
	return false
}

// 获得所有的lookup服务的http地址
func (n *NSQD) lookupdHTTPAddrs() []string {
	var lookupHTTPAddrs []string
	lookupPeers := n.lookupPeers.Load()
	if lookupPeers == nil {
		return nil
	}
	for _, lp := range lookupPeers.([]*lookupPeer) {
		if len(lp.Info.BroadcastAddress) <= 0 {
			continue
		}
		addr := net.JoinHostPort(lp.Info.BroadcastAddress, strconv.Itoa(lp.Info.HTTPPort))
		lookupHTTPAddrs = append(lookupHTTPAddrs, addr)
	}
	return lookupHTTPAddrs
}
