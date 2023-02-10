package utils

import "net"

// GetFreeTcpPort gets an unused high TCP port number. Note, this function is inherently
// racy, as another user might concurrently open the same port.
func GetFreeTcpPort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", ":0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port, nil
}
