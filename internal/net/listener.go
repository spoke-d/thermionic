package net

import (
	"fmt"
	"net"
	"strings"
)

// DefaultPort defines a default port used through out
// This should be a dependency
const DefaultPort = "8080"

// ListenAddresses returns a list of host:port combinations at which
// this machine can be reached
func ListenAddresses(value string) ([]string, error) {
	addresses := make([]string, 0)

	if value == "" {
		return addresses, nil
	}

	localHost, localPort, err := net.SplitHostPort(value)
	if err != nil {
		localHost = value
		localPort = DefaultPort
	}

	if localHost == "0.0.0.0" || localHost == "::" || localHost == "[::]" {
		ifaces, err := net.Interfaces()
		if err != nil {
			return addresses, err
		}

		for _, i := range ifaces {
			addrs, err := i.Addrs()
			if err != nil {
				continue
			}

			for _, addr := range addrs {
				var ip net.IP
				switch v := addr.(type) {
				case *net.IPNet:
					ip = v.IP
				case *net.IPAddr:
					ip = v.IP
				}

				if !ip.IsGlobalUnicast() {
					continue
				}

				if ip.To4() == nil {
					if localHost == "0.0.0.0" {
						continue
					}
					addresses = append(addresses, fmt.Sprintf("[%s]:%s", ip, localPort))
				} else {
					addresses = append(addresses, fmt.Sprintf("%s:%s", ip, localPort))
				}
			}
		}
	} else {
		if strings.Contains(localHost, ":") {
			addresses = append(addresses, fmt.Sprintf("[%s]:%s", localHost, localPort))
		} else {
			addresses = append(addresses, fmt.Sprintf("%s:%s", localHost, localPort))
		}
	}

	return addresses, nil
}
