package internal

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
)

func ParseNetworkAddr(v string) (string, string, error) {
	if v == "" {
		return "", "", errors.New("empty value")
	}
	if strings.Contains(v, ":") {
		_, port, err := net.SplitHostPort(v)
		if err != nil {
			return "", "", err
		}
		if port == "" {
			return "", "", fmt.Errorf("address %v: port is missing", v)
		}
		if _, err = strconv.ParseUint(port, 10, 16); err != nil {
			return "", "", fmt.Errorf("address %v: invalid port: %w", v, err)
		}
		return "tcp", v, nil
	}

	if portNum, err := strconv.ParseUint(v, 10, 16); err == nil {
		if portNum == 0 {
			return "", "", fmt.Errorf("address %v: invalid port: %v", v, portNum)
		}
		return "tcp", fmt.Sprintf(":%v", portNum), nil
	}

	return "unix", v, nil
}

func ParseServiceRealm(s string) (string, string) {
	service, realm, _ := strings.Cut(s, "@")
	return service, realm
}
