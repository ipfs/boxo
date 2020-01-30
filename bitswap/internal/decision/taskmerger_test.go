package decision

import (
	"testing"

	"github.com/ipfs/go-bitswap/internal/testutil"
	"github.com/ipfs/go-peertaskqueue"
	"github.com/ipfs/go-peertaskqueue/peertask"
)

func TestPushHaveVsBlock(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]

	wantHave := peertask.Task{
		Topic:    "1",
		Priority: 10,
		Work:     1,
		Data: &taskData{
			IsWantBlock:  false,
			BlockSize:    10,
			HaveBlock:    true,
			SendDontHave: false,
		},
	}
	wantBlock := peertask.Task{
		Topic:    "1",
		Priority: 10,
		Work:     10,
		Data: &taskData{
			IsWantBlock:  true,
			BlockSize:    10,
			HaveBlock:    true,
			SendDontHave: false,
		},
	}

	runTestCase := func(tasks []peertask.Task, expIsWantBlock bool) {
		tasks = cloneTasks(tasks)
		ptq := peertaskqueue.New(peertaskqueue.TaskMerger(newTaskMerger()))
		ptq.PushTasks(partner, tasks...)
		_, popped, _ := ptq.PopTasks(100)
		if len(popped) != 1 {
			t.Fatalf("Expected 1 task, received %d tasks", len(popped))
		}
		isWantBlock := popped[0].Data.(*taskData).IsWantBlock
		if isWantBlock != expIsWantBlock {
			t.Fatalf("Expected task.IsWantBlock to be %t, received %t", expIsWantBlock, isWantBlock)
		}
	}
	const wantBlockType = true
	const wantHaveType = false

	// should ignore second want-have
	runTestCase([]peertask.Task{wantHave, wantHave}, wantHaveType)
	// should ignore second want-block
	runTestCase([]peertask.Task{wantBlock, wantBlock}, wantBlockType)
	// want-have does not overwrite want-block
	runTestCase([]peertask.Task{wantBlock, wantHave}, wantBlockType)
	// want-block overwrites want-have
	runTestCase([]peertask.Task{wantHave, wantBlock}, wantBlockType)
}

func TestPushSizeInfo(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]

	wantBlockBlockSize := 10
	wantBlockDontHaveBlockSize := 0
	wantHaveBlockSize := 10
	wantHaveDontHaveBlockSize := 0
	wantBlock := peertask.Task{
		Topic:    "1",
		Priority: 10,
		Work:     10,
		Data: &taskData{
			IsWantBlock:  true,
			BlockSize:    wantBlockBlockSize,
			HaveBlock:    true,
			SendDontHave: false,
		},
	}
	wantBlockDontHave := peertask.Task{
		Topic:    "1",
		Priority: 10,
		Work:     2,
		Data: &taskData{
			IsWantBlock:  true,
			BlockSize:    wantBlockDontHaveBlockSize,
			HaveBlock:    false,
			SendDontHave: false,
		},
	}
	wantHave := peertask.Task{
		Topic:    "1",
		Priority: 10,
		Work:     1, Data: &taskData{
			IsWantBlock:  false,
			BlockSize:    wantHaveBlockSize,
			HaveBlock:    true,
			SendDontHave: false,
		},
	}
	wantHaveDontHave := peertask.Task{
		Topic:    "1",
		Priority: 10,
		Work:     1,
		Data: &taskData{
			IsWantBlock:  false,
			BlockSize:    wantHaveDontHaveBlockSize,
			HaveBlock:    false,
			SendDontHave: false,
		},
	}

	runTestCase := func(tasks []peertask.Task, expSize int, expBlockSize int, expIsWantBlock bool) {
		tasks = cloneTasks(tasks)
		ptq := peertaskqueue.New(peertaskqueue.TaskMerger(newTaskMerger()))
		ptq.PushTasks(partner, tasks...)
		_, popped, _ := ptq.PopTasks(100)
		if len(popped) != 1 {
			t.Fatalf("Expected 1 task, received %d tasks", len(popped))
		}
		if popped[0].Work != expSize {
			t.Fatalf("Expected task.Work to be %d, received %d", expSize, popped[0].Work)
		}
		td := popped[0].Data.(*taskData)
		if td.BlockSize != expBlockSize {
			t.Fatalf("Expected task.Work to be %d, received %d", expBlockSize, td.BlockSize)
		}
		if td.IsWantBlock != expIsWantBlock {
			t.Fatalf("Expected task.IsWantBlock to be %t, received %t", expIsWantBlock, td.IsWantBlock)
		}
	}

	isWantBlock := true
	isWantHave := false

	// want-block (DONT_HAVE) should have no effect on existing want-block (DONT_HAVE)
	runTestCase([]peertask.Task{wantBlockDontHave, wantBlockDontHave}, wantBlockDontHave.Work, wantBlockDontHaveBlockSize, isWantBlock)
	// want-have (DONT_HAVE) should have no effect on existing want-block (DONT_HAVE)
	runTestCase([]peertask.Task{wantBlockDontHave, wantHaveDontHave}, wantBlockDontHave.Work, wantBlockDontHaveBlockSize, isWantBlock)
	// want-block with size should update existing want-block (DONT_HAVE)
	runTestCase([]peertask.Task{wantBlockDontHave, wantBlock}, wantBlock.Work, wantBlockBlockSize, isWantBlock)
	// want-have with size should update existing want-block (DONT_HAVE) size,
	// but leave it as a want-block (ie should not change it to want-have)
	runTestCase([]peertask.Task{wantBlockDontHave, wantHave}, wantHaveBlockSize, wantHaveBlockSize, isWantBlock)

	// want-block (DONT_HAVE) size should not update existing want-block with size
	runTestCase([]peertask.Task{wantBlock, wantBlockDontHave}, wantBlock.Work, wantBlockBlockSize, isWantBlock)
	// want-have (DONT_HAVE) should have no effect on existing want-block with size
	runTestCase([]peertask.Task{wantBlock, wantHaveDontHave}, wantBlock.Work, wantBlockBlockSize, isWantBlock)
	// want-block with size should have no effect on existing want-block with size
	runTestCase([]peertask.Task{wantBlock, wantBlock}, wantBlock.Work, wantBlockBlockSize, isWantBlock)
	// want-have with size should have no effect on existing want-block with size
	runTestCase([]peertask.Task{wantBlock, wantHave}, wantBlock.Work, wantBlockBlockSize, isWantBlock)

	// want-block (DONT_HAVE) should update type and entry size of existing want-have (DONT_HAVE)
	runTestCase([]peertask.Task{wantHaveDontHave, wantBlockDontHave}, wantBlockDontHave.Work, wantBlockDontHaveBlockSize, isWantBlock)
	// want-have (DONT_HAVE) should have no effect on existing want-have (DONT_HAVE)
	runTestCase([]peertask.Task{wantHaveDontHave, wantHaveDontHave}, wantHaveDontHave.Work, wantHaveDontHaveBlockSize, isWantHave)
	// want-block with size should update existing want-have (DONT_HAVE)
	runTestCase([]peertask.Task{wantHaveDontHave, wantBlock}, wantBlock.Work, wantBlockBlockSize, isWantBlock)
	// want-have with size should update existing want-have (DONT_HAVE)
	runTestCase([]peertask.Task{wantHaveDontHave, wantHave}, wantHave.Work, wantHaveBlockSize, isWantHave)

	// want-block (DONT_HAVE) should update type and entry size of existing want-have with size
	runTestCase([]peertask.Task{wantHave, wantBlockDontHave}, wantHaveBlockSize, wantHaveBlockSize, isWantBlock)
	// want-have (DONT_HAVE) should not update existing want-have with size
	runTestCase([]peertask.Task{wantHave, wantHaveDontHave}, wantHave.Work, wantHaveBlockSize, isWantHave)
	// want-block with size should update type and entry size of existing want-have with size
	runTestCase([]peertask.Task{wantHave, wantBlock}, wantBlock.Work, wantBlockBlockSize, isWantBlock)
	// want-have should have no effect on existing want-have
	runTestCase([]peertask.Task{wantHave, wantHave}, wantHave.Work, wantHaveBlockSize, isWantHave)
}

func TestPushHaveVsBlockActive(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]

	wantBlock := peertask.Task{
		Topic:    "1",
		Priority: 10,
		Work:     10,
		Data: &taskData{
			IsWantBlock:  true,
			BlockSize:    10,
			HaveBlock:    true,
			SendDontHave: false,
		},
	}
	wantHave := peertask.Task{
		Topic:    "1",
		Priority: 10,
		Work:     1,
		Data: &taskData{
			IsWantBlock:  false,
			BlockSize:    10,
			HaveBlock:    true,
			SendDontHave: false,
		},
	}

	runTestCase := func(tasks []peertask.Task, expCount int) {
		tasks = cloneTasks(tasks)
		ptq := peertaskqueue.New(peertaskqueue.TaskMerger(newTaskMerger()))
		// ptq.PushTasks(partner, tasks...)
		var popped []*peertask.Task
		for _, task := range tasks {
			// Push the task
			// tracker.PushTasks([]peertask.Task{task})
			ptq.PushTasks(partner, task)
			// Pop the task (which makes it active)
			_, poppedTasks, _ := ptq.PopTasks(10)
			popped = append(popped, poppedTasks...)
		}
		if len(popped) != expCount {
			t.Fatalf("Expected %d tasks, received %d tasks", expCount, len(popped))
		}
	}

	// should ignore second want-have
	runTestCase([]peertask.Task{wantHave, wantHave}, 1)
	// should ignore second want-block
	runTestCase([]peertask.Task{wantBlock, wantBlock}, 1)
	// want-have does not overwrite want-block
	runTestCase([]peertask.Task{wantBlock, wantHave}, 1)
	// can't replace want-have with want-block because want-have is active
	runTestCase([]peertask.Task{wantHave, wantBlock}, 2)
}

func TestPushSizeInfoActive(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]

	wantBlock := peertask.Task{
		Topic:    "1",
		Priority: 10,
		Work:     10,
		Data: &taskData{
			IsWantBlock:  true,
			BlockSize:    10,
			HaveBlock:    true,
			SendDontHave: false,
		},
	}
	wantBlockDontHave := peertask.Task{
		Topic:    "1",
		Priority: 10,
		Work:     2,
		Data: &taskData{
			IsWantBlock:  true,
			BlockSize:    0,
			HaveBlock:    false,
			SendDontHave: false,
		},
	}
	wantHave := peertask.Task{
		Topic:    "1",
		Priority: 10,
		Work:     1,
		Data: &taskData{
			IsWantBlock:  false,
			BlockSize:    10,
			HaveBlock:    true,
			SendDontHave: false,
		},
	}
	wantHaveDontHave := peertask.Task{
		Topic:    "1",
		Priority: 10,
		Work:     1,
		Data: &taskData{
			IsWantBlock:  false,
			BlockSize:    0,
			HaveBlock:    false,
			SendDontHave: false,
		},
	}

	runTestCase := func(tasks []peertask.Task, expTasks []peertask.Task) {
		tasks = cloneTasks(tasks)
		ptq := peertaskqueue.New(peertaskqueue.TaskMerger(newTaskMerger()))
		var popped []*peertask.Task
		for _, task := range tasks {
			// Push the task
			ptq.PushTasks(partner, task)
			// Pop the task (which makes it active)
			_, poppedTasks, _ := ptq.PopTasks(10)
			popped = append(popped, poppedTasks...)
		}
		if len(popped) != len(expTasks) {
			t.Fatalf("Expected %d tasks, received %d tasks", len(expTasks), len(popped))
		}
		for i, task := range popped {
			td := task.Data.(*taskData)
			expTd := expTasks[i].Data.(*taskData)
			if td.IsWantBlock != expTd.IsWantBlock {
				t.Fatalf("Expected IsWantBlock to be %t, received %t", expTd.IsWantBlock, td.IsWantBlock)
			}
			if task.Work != expTasks[i].Work {
				t.Fatalf("Expected Size to be %d, received %d", expTasks[i].Work, task.Work)
			}
		}
	}

	// second want-block (DONT_HAVE) should be ignored
	runTestCase([]peertask.Task{wantBlockDontHave, wantBlockDontHave}, []peertask.Task{wantBlockDontHave})
	// want-have (DONT_HAVE) should be ignored if there is existing active want-block (DONT_HAVE)
	runTestCase([]peertask.Task{wantBlockDontHave, wantHaveDontHave}, []peertask.Task{wantBlockDontHave})
	// want-block with size should be added if there is existing active want-block (DONT_HAVE)
	runTestCase([]peertask.Task{wantBlockDontHave, wantBlock}, []peertask.Task{wantBlockDontHave, wantBlock})
	// want-have with size should be added if there is existing active want-block (DONT_HAVE)
	runTestCase([]peertask.Task{wantBlockDontHave, wantHave}, []peertask.Task{wantBlockDontHave, wantHave})

	// want-block (DONT_HAVE) should be added if there is existing active want-have (DONT_HAVE)
	runTestCase([]peertask.Task{wantHaveDontHave, wantBlockDontHave}, []peertask.Task{wantHaveDontHave, wantBlockDontHave})
	// want-have (DONT_HAVE) should be ignored if there is existing active want-have (DONT_HAVE)
	runTestCase([]peertask.Task{wantHaveDontHave, wantHaveDontHave}, []peertask.Task{wantHaveDontHave})
	// want-block with size should be added if there is existing active want-have (DONT_HAVE)
	runTestCase([]peertask.Task{wantHaveDontHave, wantBlock}, []peertask.Task{wantHaveDontHave, wantBlock})
	// want-have with size should be added if there is existing active want-have (DONT_HAVE)
	runTestCase([]peertask.Task{wantHaveDontHave, wantHave}, []peertask.Task{wantHaveDontHave, wantHave})

	// want-block (DONT_HAVE) should be ignored if there is existing active want-block with size
	runTestCase([]peertask.Task{wantBlock, wantBlockDontHave}, []peertask.Task{wantBlock})
	// want-have (DONT_HAVE) should be ignored if there is existing active want-block with size
	runTestCase([]peertask.Task{wantBlock, wantHaveDontHave}, []peertask.Task{wantBlock})
	// second want-block with size should be ignored
	runTestCase([]peertask.Task{wantBlock, wantBlock}, []peertask.Task{wantBlock})
	// want-have with size should be ignored if there is existing active want-block with size
	runTestCase([]peertask.Task{wantBlock, wantHave}, []peertask.Task{wantBlock})

	// want-block (DONT_HAVE) should be added if there is existing active want-have with size
	runTestCase([]peertask.Task{wantHave, wantBlockDontHave}, []peertask.Task{wantHave, wantBlockDontHave})
	// want-have (DONT_HAVE) should be ignored if there is existing active want-have with size
	runTestCase([]peertask.Task{wantHave, wantHaveDontHave}, []peertask.Task{wantHave})
	// second want-have with size should be ignored
	runTestCase([]peertask.Task{wantHave, wantHave}, []peertask.Task{wantHave})
	// want-block with size should be added if there is existing active want-have with size
	runTestCase([]peertask.Task{wantHave, wantBlock}, []peertask.Task{wantHave, wantBlock})
}

func cloneTasks(tasks []peertask.Task) []peertask.Task {
	var cp []peertask.Task
	for _, t := range tasks {
		td := t.Data.(*taskData)
		cp = append(cp, peertask.Task{
			Topic:    t.Topic,
			Priority: t.Priority,
			Work:     t.Work,
			Data: &taskData{
				IsWantBlock:  td.IsWantBlock,
				BlockSize:    td.BlockSize,
				HaveBlock:    td.HaveBlock,
				SendDontHave: td.SendDontHave,
			},
		})
	}
	return cp
}
