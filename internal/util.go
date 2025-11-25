package internal

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
)

var ErrInvalidCaller = errors.New("invalid caller header")

const SigHeader = "X-Mess-Sig"

type RotateRequest struct {
	Key string `json:"key"` // PEM
	Crt string `json:"crt"` // PEM
}

func ConstructCaller(nodeID uint64, realm, service string) string {
	var b strings.Builder
	if nodeID > 0 {
		b.WriteString(strconv.FormatUint(nodeID, 10))
	}
	b.WriteRune(';')
	if realm != "" {
		b.WriteString(realm)
	}
	b.WriteRune(';')
	if service != "" {
		b.WriteString(service)
	}
	return b.String()
}

func ParseCaller(s string) (nodeID uint64, realm string, service string, err error) {
	n, rem, ok := strings.Cut(s, ";")
	if ok {
		if n != "" {
			if nodeID, err = strconv.ParseUint(n, 10, 64); err != nil {
				return
			}
		}
		realm, service, ok = strings.Cut(rem, ";")
	}
	if !ok {
		err = ErrInvalidCaller
	}
	return
}

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

func DrainAndCloseBody(res *http.Response) {
	if res != nil {
		_, _ = io.Copy(io.Discard, res.Body)
		_ = res.Body.Close()
	}
}

func ServiceName(service, realm string) string {
	if realm == "" {
		return service
	}
	return service + "@" + realm
}
