package internal

import (
	"strings"
)

func ParseServiceRealm(s string) (string, string) {
	service, realm, _ := strings.Cut(s, "@")
	return service, realm
}
