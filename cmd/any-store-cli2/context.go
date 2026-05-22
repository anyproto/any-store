package main

import (
	"context"
	"sync"
)

var mainCtx = &contextKeeper{}

func init() {
	mainCtx.CancelAndReplace()
}

type contextKeeper struct {
	mu     sync.Mutex
	ctx    context.Context
	cancel context.CancelFunc
}

func (cc *contextKeeper) Ctx() context.Context {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	return cc.ctx
}

func (cc *contextKeeper) CancelAndReplace() {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	if cc.cancel != nil {
		cc.cancel()
	}
	cc.ctx, cc.cancel = context.WithCancel(context.Background())
}
