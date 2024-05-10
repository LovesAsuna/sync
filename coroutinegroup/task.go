package coroutinegroup

import (
	"context"
	"sync/atomic"
	"time"
)

type TaskFunc func(context.Context) error

type Task interface {
	Join() (done bool, err error)
	// MustRun forces a task to be run at least once, but it does not guarantee that the task will only run once.
	MustRun(context.Context) error
	SetRetryTimes(n int32)
	SetRetryInterval(interval time.Duration)
}

func newTask(ctx context.Context, f TaskFunc, idGenerator func() int32) *taskImpl {
	if ctx == nil {
		ctx = context.Background()
	}
	task := new(taskImpl)
	task.ctx = ctx
	task.id = idGenerator()
	task.done = make(chan token)
	task.workFunc = func(ctx context.Context) error {
		err := f(ctx)
		if err != nil && task.retryTimesLeft.Load() > 0 && task.retryInterval != 0 {
			time.Sleep(task.retryInterval)
		}
		return err
	}
	return task
}

type taskImpl struct {
	ctx            context.Context
	err            error
	workFunc       TaskFunc
	done           chan token
	id             int32
	retryTimesLeft atomic.Int32
	retryInterval  time.Duration
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
		t.doSetRetryTimes(func(oldValue int32) (newValue int32) {
			return n
		})
	}
}

func (t *taskImpl) doSetRetryTimes(f func(oldValue int32) (newValue int32)) int32 {
	for {
		oldValue := t.retryTimesLeft.Load()
		newValue := f(oldValue)
		if t.retryTimesLeft.CompareAndSwap(oldValue, newValue) {
			return newValue
		}
	}
}

func (t *taskImpl) SetRetryInterval(interval time.Duration) {
	t.retryInterval = interval
}
