package mr

import (
	"math/rand"
	"sync/atomic"
	"time"
)

type taskStatus int64

const (
	Idle taskStatus = iota
	Processing
	Moving
	TaskDone
)

const timeout = 10

type taskSet struct {
	done      bool
	len       int
	status    []taskStatus // must use CompareAndSwap to change status
	idleTasks chan int
	// worker launchtime, use for rescheduling time out task
	// timeout: 10s
	launchTs []int64
}

func (t *taskSet) Done() bool {
	if t.done {
		return true
	}
	d := true
	for _, s := range t.status {
		if s != TaskDone {
			d = false
			break
		}
	}
	if d {
		t.done = true
	}
	return t.done
}
func (t *taskSet) Len() int { return t.len }
func (t *taskSet) Init(len int) {
	if len == 0 {
		t.done = true
	}
	t.len = len
	t.status = make([]taskStatus, len)
	t.idleTasks = make(chan int, len)
	for i := 0; i < len; i++ {
		t.idleTasks <- i
	}
	t.launchTs = make([]int64, len)
}
func (t *taskSet) SetDone(i int) {
	if i >= 0 && i < t.Len() {
		t.status[i] = TaskDone
	}
}

// atomic operation, from processing to moving
func (t *taskSet) SetMoving(i int) (ok bool) {
	if i >= 0 && i < t.Len() {
		ok = atomic.CompareAndSwapInt64((*int64)(&t.status[i]), int64(Processing), int64(Moving))
		return
	}
	return false
}

func (t *taskSet) MovingFail(i int) (ok bool) {
	if i >= 0 && i < t.Len() {
		ok = atomic.CompareAndSwapInt64((*int64)(&t.status[i]), int64(Moving), int64(Processing))
		return
	}
	return false
}

func (t *taskSet) GetIdleTask() (task int, cmd WorkerCommand) {
	if t.Done() {
		return -1, WorkerCmdDone
	}

	select {
	case task = <-t.idleTasks:
		t.status[task] = Processing
		t.launchTs[task] = time.Now().Unix()
		return task, WorkerCmdProcess
	default:
		// eager scheduling, timeout tasks are taken preferentially
		now := time.Now().Unix()
		timeoutTasks := make([]int, 0, t.len/2)
		unFinishTasks := make([]int, 0, t.len/2)
		for i := 0; i < t.len; i++ {
			if t.status[i] != TaskDone && now-t.launchTs[i] > timeout {
				timeoutTasks = append(timeoutTasks, i)
			}
			if t.status[i] != TaskDone {
				unFinishTasks = append(unFinishTasks, i)
			}
		}
		if len(timeoutTasks) != 0 {
			task := timeoutTasks[rand.Intn(len(timeoutTasks))]
			ok := atomic.CompareAndSwapInt64((*int64)(&t.status[task]), int64(Processing), int64(Processing))
			if !ok {
				return -1, WorkerCmdWait
			}
			t.launchTs[task] = time.Now().Unix()
			return task, WorkerCmdProcess
		}
		// disable backup task on eager mode, or some test fail
		// --- map jobs ran incorrect number of times (9 != 8)
		// --- job count test: FAIL
		// if len(unFinishTasks) != 0 {
		// 	task := unFinishTasks[rand.Intn(len(unFinishTasks))]
		// 	ok := atomic.CompareAndSwapInt64((*int64)(&t.status[task]), int64(Processing), int64(Processing))
		// 	if !ok {
		// 		return -1, WorkerCmdWait
		// 	}
		// 	t.launchTs[task] = time.Now().Unix()
		// 	return task, WorkerCmdProcess
		// }
		// goes here due to concurrency operation, now all is moving or done
		if t.Done() {
			return -1, WorkerCmdDone
		}
		return -1, WorkerCmdWait
	}
}
