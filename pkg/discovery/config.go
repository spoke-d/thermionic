package discovery

// config defines a configuration setup for creating a list to manage the
// members cluster
type config struct {
	bindAddr      string
	bindPort      int
	advertiseAddr string
	advertisePort int
	daemonAddress string
	daemonNonce   string
}

// Config defines a option for generating a filesystem config
type Config func(*config)

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

// WithDaemon adds a DaemonAddress and DaemonNonce to the configuration
func WithDaemon(address, nonce string) Config {
	return func(config *config) {
		config.daemonAddress = address
		config.daemonNonce = nonce
	}
}

// Create a config instance with default values.
func newConfigs() *config {
	return &config{}
}
