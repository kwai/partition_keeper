package utils

import "sync"

type LooseLock struct {
	mu   sync.RWMutex
	cond *sync.Cond
	held bool
}

func NewLooseLock() *LooseLock {
	ans := &LooseLock{
		mu:   sync.RWMutex{},
		cond: nil,
		held: false,
	}
	ans.cond = sync.NewCond(&(ans.mu))
	return ans
}

func (p *LooseLock) LockWrite() {
	p.mu.Lock()
	for p.held {
		p.cond.Wait()
	}
	p.held = true
}

func (p *LooseLock) UnlockWrite() {
	p.held = false
	p.cond.Signal()
	p.mu.Unlock()
}

func (p *LooseLock) AllowRead() {
	p.mu.Unlock()
}

func (p *LooseLock) DisallowRead() {
	p.mu.Lock()
}

func (p *LooseLock) LockRead() {
	p.mu.RLock()
}

func (p *LooseLock) UnlockRead() {
	p.mu.RUnlock()
}
