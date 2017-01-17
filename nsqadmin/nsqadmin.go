package nsqadmin

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"sync"
	"sync/atomic"

	"github.com/nsqio/nsq/internal/http_api"
	"github.com/nsqio/nsq/internal/util"
	"github.com/nsqio/nsq/internal/version"
)

// nsqadmin的数据结构
type NSQAdmin struct {
	sync.RWMutex
	opts                atomic.Value
	httpListener        net.Listener
	waitGroup           util.WaitGroupWrapper
	notifications       chan *AdminAction
	graphiteURL         *url.URL
	httpClientTLSConfig *tls.Config
}

// nsqadmin的初始化工作
func New(opts *Options) *NSQAdmin {
	// 声明和初始化nsqadmin数据结构
	n := &NSQAdmin{
		notifications: make(chan *AdminAction),
	}
	// 原子操作存储nsqadmin的配置信息
	n.swapOpts(opts)

	// nsqdlookup和nsqd的http地址不能都为空
	if len(opts.NSQDHTTPAddresses) == 0 && len(opts.NSQLookupdHTTPAddresses) == 0 {
		n.logf("--nsqd-http-address or --lookupd-http-address required.")
		os.Exit(1)
	}

	// nsqdlookup和nsqd的http地址不能都有配置信息，只需要配置nsqdlookup或者nsqd的http地址
	if len(opts.NSQDHTTPAddresses) != 0 && len(opts.NSQLookupdHTTPAddresses) != 0 {
		n.logf("use --nsqd-http-address or --lookupd-http-address not both")
		os.Exit(1)
	}

	// verify that the supplied address is valid
	// 验证http地址正确性的函数
	verifyAddress := func(arg string, address string) *net.TCPAddr {
		addr, err := net.ResolveTCPAddr("tcp", address)
		if err != nil {
			n.logf("FATAL: failed to resolve %s address (%s) - %s", arg, address, err)
			os.Exit(1)
		}
		return addr
	}

	if opts.HTTPClientTLSCert != "" && opts.HTTPClientTLSKey == "" {
		n.logf("FATAL: --http-client-tls-key must be specified with --http-client-tls-cert")
		os.Exit(1)
	}

	if opts.HTTPClientTLSKey != "" && opts.HTTPClientTLSCert == "" {
		n.logf("FATAL: --http-client-tls-cert must be specified with --http-client-tls-key")
		os.Exit(1)
	}

	n.httpClientTLSConfig = &tls.Config{
		InsecureSkipVerify: opts.HTTPClientTLSInsecureSkipVerify,
	}
	if opts.HTTPClientTLSCert != "" && opts.HTTPClientTLSKey != "" {
		cert, err := tls.LoadX509KeyPair(opts.HTTPClientTLSCert, opts.HTTPClientTLSKey)
		if err != nil {
			n.logf("FATAL: failed to LoadX509KeyPair %s, %s - %s",
				opts.HTTPClientTLSCert, opts.HTTPClientTLSKey, err)
			os.Exit(1)
		}
		n.httpClientTLSConfig.Certificates = []tls.Certificate{cert}
	}
	if opts.HTTPClientTLSRootCAFile != "" {
		tlsCertPool := x509.NewCertPool()
		caCertFile, err := ioutil.ReadFile(opts.HTTPClientTLSRootCAFile)
		if err != nil {
			n.logf("FATAL: failed to read TLS root CA file %s - %s",
				opts.HTTPClientTLSRootCAFile, err)
			os.Exit(1)
		}
		if !tlsCertPool.AppendCertsFromPEM(caCertFile) {
			n.logf("FATAL: failed to AppendCertsFromPEM %s", opts.HTTPClientTLSRootCAFile)
			os.Exit(1)
		}
		n.httpClientTLSConfig.RootCAs = tlsCertPool
	}

	// require that both the hostname and port be specified
	// 验证配置的nsqdlookup的http地址的正确性
	for _, address := range opts.NSQLookupdHTTPAddresses {
		verifyAddress("--lookupd-http-address", address)
	}

	// 验证配置的nsqd的http地址的正确性
	for _, address := range opts.NSQDHTTPAddresses {
		verifyAddress("--nsqd-http-address", address)
	}

	if opts.ProxyGraphite {
		url, err := url.Parse(opts.GraphiteURL)
		if err != nil {
			n.logf("FATAL: failed to parse --graphite-url='%s' - %s", opts.GraphiteURL, err)
			os.Exit(1)
		}
		n.graphiteURL = url
	}

	// 打印当前nsqadmin的版本号
	n.logf(version.String("nsqadmin"))

	return n
}

// nsqadmin的日志打印函数
func (n *NSQAdmin) logf(f string, args ...interface{}) {
	if n.getOpts().Logger == nil {
		return
	}
	n.getOpts().Logger.Output(2, fmt.Sprintf(f, args...))
}

// 原子操作获得nsqadmin的配置信息
func (n *NSQAdmin) getOpts() *Options {
	return n.opts.Load().(*Options)
}

// 院子操作存储nsqadmin的配置信息
func (n *NSQAdmin) swapOpts(opts *Options) {
	n.opts.Store(opts)
}

// 获得nsqadmin监听的http地址
func (n *NSQAdmin) RealHTTPAddr() *net.TCPAddr {
	n.RLock()
	defer n.RUnlock()
	return n.httpListener.Addr().(*net.TCPAddr)
}

func (n *NSQAdmin) handleAdminActions() {
	for action := range n.notifications {
		content, err := json.Marshal(action)
		if err != nil {
			n.logf("ERROR: failed to serialize admin action - %s", err)
		}
		httpclient := &http.Client{
			Transport: http_api.NewDeadlineTransport(n.getOpts().HTTPClientConnectTimeout, n.getOpts().HTTPClientRequestTimeout),
		}
		n.logf("POSTing notification to %s", n.getOpts().NotificationHTTPEndpoint)
		resp, err := httpclient.Post(n.getOpts().NotificationHTTPEndpoint,
			"application/json", bytes.NewBuffer(content))
		if err != nil {
			n.logf("ERROR: failed to POST notification - %s", err)
		}
		resp.Body.Close()
	}
}

// nsqadmin的主运行函数
func (n *NSQAdmin) Main() {
	// 监听配置的http地址
	httpListener, err := net.Listen("tcp", n.getOpts().HTTPAddress)
	if err != nil {
		n.logf("FATAL: listen (%s) failed - %s", n.getOpts().HTTPAddress, err)
		os.Exit(1)
	}
	// 存储http监听句柄
	n.Lock()
	n.httpListener = httpListener
	n.Unlock()
	// 初始化http数据结构
	httpServer := NewHTTPServer(&Context{n})
	n.waitGroup.Wrap(func() {
		http_api.Serve(n.httpListener, http_api.CompressHandler(httpServer), "HTTP", n.getOpts().Logger)
	})
	n.waitGroup.Wrap(func() { n.handleAdminActions() })
}

// nsqadmin的退出函数
func (n *NSQAdmin) Exit() {
	n.httpListener.Close()
	close(n.notifications)
	n.waitGroup.Wait()
}
