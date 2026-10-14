package lifecycle

import (
	"context"
	"sync"
	"sync/atomic"
)

type Controller struct {
	draining atomic.Bool
	wg       sync.WaitGroup
	stopCh   chan struct{}
}

func NewController() *Controller {
	return &Controller{stopCh: make(chan struct{})}
}

func (c *Controller) Draining() bool {
	return c.draining.Load()
}

func (c *Controller) BeginDrain() {
	c.draining.Store(true)
}

func (c *Controller) StopCh() <-chan struct{} {
	return c.stopCh
}

func (c *Controller) Close() {
	select {
	case <-c.stopCh:
	default:
		close(c.stopCh)
	}
}

func (c *Controller) Go(fn func()) {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		fn()
	}()
}

func (c *Controller) Wait(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
