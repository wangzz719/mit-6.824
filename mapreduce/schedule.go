package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).

	// Part III
	//shecduleJobPartIII(jobName, mapFiles, ntasks, n_other, phase, registerChan)

	// Part IV
	shecduleJobPartIV(jobName, mapFiles, ntasks, n_other, phase, registerChan)

	fmt.Printf("Schedule: %v done\n", phase)
}

func shecduleJobPartIII(jobName string, mapFiles []string, phaseJobCount int, otherPhaseJobCount int, phase jobPhase, registerChan chan string) {
	var wg sync.WaitGroup
	wg.Add(phaseJobCount)

	for i := 0; i < phaseJobCount; i++ {
		arg := DoTaskArgs{
			JobName:       jobName,
			Phase:         phase,
			TaskNumber:    i,
			NumOtherPhase: otherPhaseJobCount,
		}
		if phase == mapPhase {
			arg.File = mapFiles[i]
		}
		addr := <-registerChan
		go func() {
			call(
				addr,
				"Worker.DoTask",
				arg,
				nil)
			wg.Done()
			registerChan <- addr
		}()
	}

	wg.Wait()
}

func shecduleJobPartIV(jobName string, mapFiles []string, phaseJobCount int, otherPhaseJobCount int, phase jobPhase, registerChan chan string) {
	var wg sync.WaitGroup
	wg.Add(phaseJobCount)

	tasks := make(chan *DoTaskArgs, phaseJobCount)

	for i := 0; i < phaseJobCount; i++ {
		arg := DoTaskArgs{
			JobName:       jobName,
			Phase:         phase,
			TaskNumber:    i,
			NumOtherPhase: otherPhaseJobCount,
		}
		if phase == mapPhase {
			arg.File = mapFiles[i]
		}
		tasks <- &arg
	}
	fmt.Println("tasks Len", len(tasks))
	go func() {
		for arg := range tasks {
			arg := arg // closure
			addr := <-registerChan
			go func() {
				ok := call(
					addr,
					"Worker.DoTask",
					arg,
					nil)
				if ok {
					wg.Done()
					registerChan <- addr
				} else {
					tasks <- arg
				}
			}()
		}
	}()
	wg.Wait()
	close(tasks)
}
