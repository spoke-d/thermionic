package members

import (
	"io"
	"time"
)

// config defines a configuration setup for creating a list to manage the
// members cluster
type config struct {
	peerType         PeerType
	nodeName         string
	daemonAddress    string
	daemonNonce      string
	bindAddr         string
	bindPort         int
	advertiseAddr    string
	advertisePort    int
	clientAddr       string
	clientPort       int
	logOutput        io.Writer
	broadcastTimeout time.Duration
}

// Config defines a option for generating a filesystem config
type Config func(*config)

// WithPeerType adds a PeerType to the configuration
func WithPeerType(peerType PeerType) Config {
	return func(config *config) {
		config.peerType = peerType
	}
}

// WithNodeName adds a NodeName to the configuration
func WithNodeName(nodeName string) Config {
	return func(config *config) {
		config.nodeName = nodeName
	}
}

// WithDaemon adds a DaemonAddress, DaemonNonce to the configuration
func WithDaemon(address, nonce string) Config {
	return func(config *config) {
		config.daemonAddress = address
		config.daemonNonce = nonce
	}
}

// WithBindAddrPort adds a BindAddr and BindPort to the configuration
func WithBindAddrPort(addr string, port int) Config {
	return func(config *config) {
		config.bindAddr = addr
		config.bindPort = port
	}
}

// WithAdvertiseAddrPort adds a AdvertiseAddr and AdvertisePort to the configuration
func WithAdvertiseAddrPort(addr string, port int) Config {
	return func(config *config) {
		config.advertiseAddr = addr
		config.advertisePort = port
	}
}

// WithClientAddrPort adds a ClientAddr and ClientPort to the configuration
func WithClientAddrPort(addr string, port int) Config {
	return func(config *config) {
		config.clientAddr = addr
		config.clientPort = port
	}
}

// WithLogOutput adds a LogOutput to the configuration
func WithLogOutput(logOutput io.Writer) Config {
	return func(config *config) {
		config.logOutput = logOutput
	}
}

// WithBroadcastTimeout adds a BroadcastTimeout to the configuration
func WithBroadcastTimeout(d time.Duration) Config {
	return func(config *config) {
		config.broadcastTimeout = d
	}
}

// Create a config instance with default values.
func newConfigs() *config {
	return &config{}
}
