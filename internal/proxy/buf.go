package proxy

import "sync"

type bufPool struct {
	pool sync.Pool
}

var BufferPool = &bufPool{
	pool: sync.Pool{
		New: func() any {
			buf := make([]byte, 64*1024)
			return &buf
		},
	},
}

func (p *bufPool) Get() []byte  { return *p.pool.Get().(*[]byte) }
func (p *bufPool) Put(b []byte) { p.pool.Put(&b) }
