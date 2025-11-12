package internal

import (
	"encoding/json"
	"os"
)

func WriteObject(filename string, v any) error {
	b, err := json.MarshalIndent(v, "", "\t")
	if err != nil {
		return err
	}
	return WriteFile(filename, b)
}

func ReadObject(filename string, v any) error {
	b, err := ReadFile(filename)
	if err != nil {
		return err
	}
	if err = json.Unmarshal(b, v); err != nil {
		return err
	}
	return nil
}

func LoadObject[T any](filename string) (*T, error) {
	b, err := ReadFile(filename)
	if err != nil {
		return nil, err
	}
	v := new(T)
	if err = json.Unmarshal(b, v); err != nil {
		return nil, err
	}
	return v, nil
}

func ReadFile(filename string) ([]byte, error) {
	return os.ReadFile(filename)
}

func WriteFile(filename string, data []byte) error {
	tempname := filename + ".temp"
	if err := os.WriteFile(tempname, data, 0600); err != nil {
		return err
	}
	return os.Rename(tempname, filename)
}
