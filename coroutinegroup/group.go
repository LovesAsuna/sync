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
	groupImpl struct {
		ctx                 context.Context
		workerChan          chan *taskImpl
		sem                 chan token
		cancel              func() (first bool)
		errChan             chan error
		errs                []error
		taskWg              sync.WaitGroup
		collectErrorDone    chan token
		globalRetryTimes    int32
		globalRetryInterval time.Duration
		once                sync.Once
		idGenerator         atomic.Int32
	}
)

// WithContext create a groupImpl with context. It returns a groupImpl and a cancelable context.
func WithContext(ctx context.Context) (Group, context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	canceled := new(atomic.Bool)
	cw := &groupImpl{
		ctx:              ctx,
		workerChan:       make(chan *taskImpl),
		errChan:          make(chan error),
		collectErrorDone: make(chan token),
	}
	cw.cancel = func() (first bool) {
		cancel()
		return canceled.CompareAndSwap(false, true)
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

// SetGlobalRetryInterval set each task's retry interval.
func (c *groupImpl) SetGlobalRetryInterval(interval time.Duration) Group {
	c.globalRetryInterval = interval
	return c
}

// SetMaxErrorTask Wait will return when number of error tasks reached n, even if not all tasks are completed.
// default to not allow any error
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
				_ = c.cancel()
				c.errChan <- t.err
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
			c.cancel()
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
	defer close(c.collectErrorDone)
	if c.cancel() {
		for {
			select {
			case err := <-c.errChan:
				c.errs = append(c.errs, err)
			default:
				return
			}
		}
	}
	for err := range c.errChan {
		c.errs = append(c.errs, err)
		// an extra error cause Wait return
		if len(c.errs) == cap(c.errChan)+1 {
			break
		}
	}
}
