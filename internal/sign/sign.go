package sign

import (
	"crypto/ed25519"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"time"
)

const Header = "X-Mess-Sig"

func Timestamp(key ed25519.PrivateKey) string {
	data := make([]byte, 8, 48)
	binary.BigEndian.PutUint64(data, uint64(time.Now().Unix()))
	b := append(data, ed25519.Sign(key, data)...)
	return hex.EncodeToString(b)
}

func Verify(publicKey ed25519.PublicKey, value string) error {
	data, err := hex.DecodeString(value)
	if err != nil {
		return errors.New("signature verification failed")
	}
	if len(data) <= 8 {
		return errors.New("invalid data len")
	}

	if !ed25519.Verify(publicKey, data[:8], data[8:]) {
		return errors.New("signature verification failed")
	}

	ts := int64(binary.BigEndian.Uint64(data[:8]))

	if time.Since(time.Unix(ts, 0)) > 30*time.Second {
		return errors.New("signature verification failed")
	}

	return nil
}
