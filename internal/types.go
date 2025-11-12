package internal

import (
	"mess"
)

type (
	RotateRequest struct {
		Key string `json:"key"` // PEM
		Crt string `json:"crt"` // PEM
	}

	ServiceCommand struct {
		Service string `json:"service"`
		Realm   string `json:"realm"`
		Timeout int    `json:"timeout"` // seconds
	}

	LogsRequest struct {
		Realm   string `json:"realm"`
		Service string `json:"service"`
		Offset  uint64 `json:"offset"`
		Limit   uint64 `json:"limit"`
	}

	LogsResponse struct {
		Logs []*mess.LogRecord `json:"logs"`
	}
)
