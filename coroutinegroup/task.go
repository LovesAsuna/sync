package coroutinegroup

import (
	"context"
	"sync/atomic"
)

type TaskFunc func(context.Context) error

type Task interface {
	Join() (done bool, err error)
	// MustRun forces a task to be run at least once, but it does not guarantee that the task will only run once.
	MustRun(context.Context) error
	SetRetryTimes(n int32)
}

func newTask(ctx context.Context, f TaskFunc, idGenerator func() int32) *taskImpl {
	if ctx == nil {
		ctx = context.Background()
	}
	return &taskImpl{ctx: ctx, workFunc: f, id: idGenerator(), done: make(chan token)}
}

type taskImpl struct {
	ctx            context.Context
	err            error
	workFunc       TaskFunc
	done           chan token
	id             int32
	retryTimesLeft atomic.Int32
}

func (t *taskImpl) Join() (done bool, err error) {
	select {
	case <-t.ctx.Done():
		return false, t.err
	case <-t.done:
		return true, t.err
	}
}

func (t *taskImpl) MustRun(ctx context.Context) error {
	done, err := t.Join()
	if done {
		return err
	}
	return t.workFunc(ctx)
}

func (t *taskImpl) SetRetryTimes(n int32) {
	select {
	case <-t.ctx.Done():
	case <-t.done:
	default:
		t.doSetRetryTimes(func(old int32) (new int32) {
			return n
		})
	}
}

func (t *taskImpl) doSetRetryTimes(f func(old int32) (new int32)) int32 {
	for {
		old := t.retryTimesLeft.Load()
		new := f(old)
		if t.retryTimesLeft.CompareAndSwap(old, new) {
			return new
		}
	}
}
