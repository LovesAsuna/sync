package coroutinegroup

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type (
	token struct{}

	// A Group manages a set of goroutines and their lifetimes.
	//
	// Group's methods may be called by multiple goroutines simultaneously.
	Group interface {
		// SetLimit limits the number of active goroutines in this group to at most n.
		// A negative value indicates no limit.
		//
		// The limit must not be modified while any goroutines in the group are active.
		SetLimit(n int32) Group

		// SetGlobalRetryTimes set each task's retry times.
		SetGlobalRetryTimes(n int32) Group

		// SetGlobalRetryInterval set each task's retry interval.
		SetGlobalRetryInterval(interval time.Duration) Group

		// SetMaxErrorTask Wait will return when number of error tasks reached n, even if not all tasks are completed.
		// default to not allow any error
		SetMaxErrorTask(n int32) Group

		// Wait tasks from Go are designed to be actually executed only when a Wait is called.
		// Wait will return only all tasks are completed successfully or the number of error tasks reached n.
		Wait() []error

		// Go run a single task and return a task handle.
		Go(f TaskFunc) Task
	}
	errStruct struct {
		errChan          chan error
		errs             []error
		collectErrorDone chan token
	}
	syncStruct struct {
		sem    chan token
		taskWg sync.WaitGroup
		once   sync.Once
	}
	taskStruct struct {
		globalRetryTimes    int32
		globalRetryInterval time.Duration
		idGenerator         atomic.Int32
	}
	groupImpl struct {
		errStruct
		syncStruct
		taskStruct
		ctx        context.Context
		workerChan chan *taskImpl
		cancel     func(cause error)
		canceled   atomic.Bool
	}
)

// WithContext create a groupImpl with context. It returns a groupImpl and a cancelable context.
func WithContext(ctx context.Context) (Group, context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	cw := &groupImpl{
		ctx:        ctx,
		workerChan: make(chan *taskImpl),
		errStruct: errStruct{
			errChan:          make(chan error),
			collectErrorDone: make(chan token),
		},
	}
	cw.cancel = func(cause error) {
		if cw.canceled.CompareAndSwap(false, true) {
			cancel()
			if cause != nil {
				cw.errChan <- cause
			}
		}
	}
	return cw, ctx
}

func (c *groupImpl) SetLimit(n int32) Group {
	if n < 0 {
		c.sem = nil
		return c
	}
	if len(c.sem) != 0 {
		panic(fmt.Errorf("coroutine groupImpl: modify limit while %v goroutines in the groupImpl are still active", len(c.sem)))
	}
	c.sem = make(chan token, n)
	return c
}

func (c *groupImpl) SetGlobalRetryTimes(n int32) Group {
	c.globalRetryTimes = n
	return c
}

func (c *groupImpl) SetGlobalRetryInterval(interval time.Duration) Group {
	c.globalRetryInterval = interval
	return c
}

func (c *groupImpl) SetMaxErrorTask(n int32) Group {
	c.errChan = make(chan error, n)
	return c
}

func (c *groupImpl) pushTask(task *taskImpl) Task {
	c.taskWg.Add(1)
	go func() {
		// require sem
		if c.sem != nil {
			c.sem <- token{}
		}
		select {
		case <-c.ctx.Done():
			// release sem when context is canceled
			if c.sem != nil {
				<-c.sem
			}
			c.taskWg.Done()
			return
		case c.workerChan <- task:
		}
	}()
	return task
}

func (c *groupImpl) Go(f TaskFunc) Task {
	t := newTask(c.ctx, f, func() int32 {
		return c.idGenerator.Add(1)
	})
	t.SetRetryTimes(c.globalRetryTimes)
	t.SetRetryInterval(c.globalRetryInterval)
	return c.pushTask(t)
}

func (c *groupImpl) dispatchTask(t *taskImpl) {
	defer func() {
		c.taskWg.Done()
	}()
	// run task and catch error or any error panic
	t.err = func() (err error) {
		defer func() {
			r := recover()
			if e, ok := r.(error); ok {
				err = e
			}
			if s, ok := r.(string); ok {
				err = errors.New(s)
			}
		}()
		err = t.workFunc(c.ctx)
		return
	}()
	// release sem
	if c.sem != nil {
		<-c.sem
	}
	if t.err != nil {
		if t.doSetRetryTimes(func(old int32) (new int32) { return old - 1 }) >= 0 {
			c.pushTask(t)
		} else {
			select {
			case <-c.ctx.Done():
			case c.errChan <- t.err:
			default:
				c.cancel(t.err)
			}
		}
	} else {
		close(t.done)
	}
}

func (c *groupImpl) Wait() []error {
	c.once.Do(func() {
		go func() {
			c.taskWg.Wait()
			c.cancel(nil)
			close(c.workerChan)
			close(c.errChan)
			if c.sem != nil {
				close(c.sem)
			}
		}()
		go func() {
			for t := range c.workerChan {
				go c.dispatchTask(t)
			}
		}()
		<-c.ctx.Done()
		c.collectError()
	})
	<-c.collectErrorDone
	return c.errs
}

func (c *groupImpl) collectError() {
	defer func() {
		go func() {
			for range c.errChan {
			}
		}()
		close(c.collectErrorDone)
	}()
	// external cancel(Non-blocking collection)
	if !c.canceled.Load() {
		if err := context.Cause(c.ctx); err != nil {
			c.errs = append(c.errs, err)
		}
		for {
			length := len(c.errChan)
			if length == 0 {
				break
			}
			for i := 0; i < length; i++ {
				err := <-c.errChan
				c.errs = append(c.errs, err)
			}
		}
		return
	}
	// internal cancel(blocking collection)
	for err := range c.errChan {
		c.errs = append(c.errs, err)
		// an additional error is causing the Wait to return, but due to coroutine race condition, this error might not be the real cause
		if len(c.errs) == cap(c.errChan)+1 {
			break
		}
	}
}
