package main

import (
	"container/heap"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/olekukonko/tablewriter"
)

func main() {
	// CLI args
	f, closeFile, err := openProcessingFile(os.Args...)
	if err != nil {
		log.Fatal(err)
	}
	defer closeFile()

	// Load and parse processes
	processes, err := loadProcesses(f)
	if err != nil {
		log.Fatal(err)
	}

	// First-come, first-serve scheduling
	//FCFSSchedule(os.Stdout, "First-come, first-serve", processes)

	//SJFSchedule(os.Stdout, "Shortest-job-first", processes)

	SJFPrioritySchedule(os.Stdout, "Priority", processes)

	//RRSchedule(os.Stdout, "Round-robin", processes)
}

func openProcessingFile(args ...string) (*os.File, func(), error) {
	if len(args) != 2 {
		return nil, nil, fmt.Errorf("%w: must give a scheduling file to process", ErrInvalidArgs)
	}
	// Read in CSV process CSV file
	f, err := os.Open(args[1])
	if err != nil {
		return nil, nil, fmt.Errorf("%v: error opening scheduling file", err)
	}
	closeFn := func() {
		if err := f.Close(); err != nil {
			log.Fatalf("%v: error closing scheduling file", err)
		}
	}

	return f, closeFn, nil
}

type (
	Process struct {
		ProcessID     int64
		ArrivalTime   int64
		BurstDuration int64
		Priority      int64
	}
	TimeSlice struct {
		PID   int64
		Start int64
		Stop  int64
	}
)

//region Schedulers

// FCFSSchedule outputs a schedule of processes in a GANTT chart and a table of timing given:
// • an output writer
// • a title for the chart
// • a slice of processes
func FCFSSchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)
	for i := range processes {
		if processes[i].ArrivalTime > 0 {
			waitingTime = serviceTime - processes[i].ArrivalTime
		}
		totalWait += float64(waitingTime)

		start := waitingTime + processes[i].ArrivalTime

		turnaround := processes[i].BurstDuration + waitingTime
		totalTurnaround += float64(turnaround)

		completion := processes[i].BurstDuration + processes[i].ArrivalTime + waitingTime
		lastCompletion = float64(completion)

		schedule[i] = []string{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].BurstDuration),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(waitingTime),
			fmt.Sprint(turnaround),
			fmt.Sprint(completion),
		}
		serviceTime += processes[i].BurstDuration

		gantt = append(gantt, TimeSlice{
			PID:   processes[i].ProcessID,
			Start: start,
			Stop:  serviceTime,
		})
	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

func SJFPrioritySchedule(w io.Writer, title string, processes []Process) {
	var (
		n                  = len(processes)
		turnAroundTimes    = make([]int64, n)
		waitingTimes       = make([]int64, n)
		gantt              = make([]TimeSlice, 0)
		lastCompletion     int64
		schedule           = make([][]string, n)
		insertedProcess    = 0
		processesMapIdx    = make(map[int64]int)
		totalBurstDuration int64
		currentTime        = processes[0].ArrivalTime
		minHeap            = &ProcessMinHeap{}
		tempBurstDuration  = make([]int64, n)
	)

	sort.Slice(processes, func(i, j int) bool {
		return processes[i].ArrivalTime < processes[j].ArrivalTime
	})

	heap.Init(minHeap)

	for i, p := range processes {
		processesMapIdx[p.ProcessID] = i
		totalBurstDuration += p.BurstDuration
		tempBurstDuration[i] = p.BurstDuration
	}

	for {
		if insertedProcess < n {
			for _, p := range processes {
				if p.ArrivalTime != currentTime {
					continue
				}
				insertedProcess++
				heap.Push(minHeap, p)
			}
		}

		pMinPriority := heap.Pop(minHeap).(Process)
		pMinPriority.BurstDuration--
		gantt = appendGantt(gantt, pMinPriority, currentTime)
		currentTime++
		gantt = setStop(gantt, currentTime)
		if pMinPriority.BurstDuration > 0 {
			heap.Push(minHeap, pMinPriority)
		} else {
			idx := processesMapIdx[pMinPriority.ProcessID]
			turnAroundTimes[idx] = currentTime - pMinPriority.ArrivalTime
			waitingTimes[idx] = turnAroundTimes[idx] - tempBurstDuration[idx]
		}

		if minHeap.Len() == 0 && insertedProcess == n {
			break
		}
	}

	for i := range processes {
		completion := processes[i].BurstDuration + processes[i].ArrivalTime + waitingTimes[i]
		if lastCompletion < completion {
			lastCompletion = completion
		}
		schedule[i] = []string{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].BurstDuration),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(waitingTimes[i]),
			fmt.Sprint(turnAroundTimes[i]),
			fmt.Sprint(completion),
		}
	}

	aveWait := float64(sum(waitingTimes)) / float64(n)
	aveTurnaround := float64(sum(turnAroundTimes)) / float64(n)
	aveThroughput := float64(n) / float64(lastCompletion)

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

// Implement min-heap

type ProcessMinHeap []Process

func (h ProcessMinHeap) Len() int {
	return len(h)
}

func (h ProcessMinHeap) Less(i, j int) bool {
	return h[i].Priority < h[j].Priority
}

func (h ProcessMinHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *ProcessMinHeap) Push(x interface{}) {
	*h = append(*h, x.(Process))
}

func (h *ProcessMinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// end min-heap

func SJFSchedule(w io.Writer, title string, processes []Process) {
	var (
		n                = len(processes)
		remainTime       = make([]int64, n)
		t                int64
		curIdx           int
		gantt            = make([]TimeSlice, 0)
		numsOfCompletion = 0
		minRemainTime    = int64(math.MaxInt64)
		isCheck          = false
		waitingTimes     = make([]int64, n)
		lastCompletion   int64
		schedule         = make([][]string, n)
	)

	for i := range processes {
		remainTime[i] = processes[i].BurstDuration
	}

	for numsOfCompletion < n {
		for i := range processes {
			if processes[i].ArrivalTime <= t && remainTime[i] < minRemainTime && remainTime[i] > 0 {
				minRemainTime = remainTime[i]
				curIdx = i
				isCheck = true
			}
		}

		if !isCheck {
			t++
			continue
		}

		gantt = appendGantt(gantt, processes[curIdx], t)

		remainTime[curIdx]--

		minRemainTime = remainTime[curIdx]
		if minRemainTime == 0 {
			minRemainTime = int64(math.MaxInt64)
		}

		if remainTime[curIdx] == 0 {
			numsOfCompletion++
			isCheck = false

			stop := t + 1
			gantt = setStop(gantt, stop)

			waitingTimes[curIdx] = stop - processes[curIdx].BurstDuration - processes[curIdx].ArrivalTime
			if waitingTimes[curIdx] < 0 {
				waitingTimes[curIdx] = 0
			}
		}

		t++
	}

	turnAroundTimes := findTurnAroundTimes(processes, waitingTimes)

	for i := range processes {
		completion := processes[i].BurstDuration + processes[i].ArrivalTime + waitingTimes[i]
		if lastCompletion < completion {
			lastCompletion = completion
		}
		schedule[i] = []string{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].BurstDuration),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(waitingTimes[i]),
			fmt.Sprint(turnAroundTimes[i]),
			fmt.Sprint(completion),
		}
	}

	aveWait := float64(sum(waitingTimes)) / float64(n)
	aveTurnaround := float64(sum(turnAroundTimes)) / float64(n)
	aveThroughput := float64(n) / float64(lastCompletion)

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

func findTurnAroundTimes(processes []Process, waitingTimes []int64) []int64 {
	turnAroundTimes := make([]int64, len(processes))
	for i := range processes {
		turnAroundTimes[i] = processes[i].BurstDuration + waitingTimes[i]
	}
	return turnAroundTimes
}

func appendGantt(gantt []TimeSlice, process Process, start int64) []TimeSlice {
	if len(gantt) == 0 || gantt[len(gantt)-1].PID != process.ProcessID {
		timeSlice := TimeSlice{
			PID:   process.ProcessID,
			Start: start,
		}
		gantt = append(gantt, timeSlice)
	}
	return gantt
}

func setStop(gantt []TimeSlice, stop int64) []TimeSlice {
	if len(gantt) > 0 {
		gantt[len(gantt)-1].Stop = stop
	}
	return gantt
}

func sum(nums []int64) int64 {
	var s int64
	for _, n := range nums {
		s += n
	}
	return s
}

//func RRSchedule(w io.Writer, title string, processes []Process) { }

//endregion

//region Output helpers

func outputTitle(w io.Writer, title string) {
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
	_, _ = fmt.Fprintln(w, strings.Repeat(" ", len(title)/2), title)
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
}

func outputGantt(w io.Writer, gantt []TimeSlice) {
	_, _ = fmt.Fprintln(w, "Gantt schedule")
	_, _ = fmt.Fprint(w, "|")
	for i := range gantt {
		pid := fmt.Sprint(gantt[i].PID)
		padding := strings.Repeat(" ", (8-len(pid))/2)
		_, _ = fmt.Fprint(w, padding, pid, padding, "|")
	}
	_, _ = fmt.Fprintln(w)
	for i := range gantt {
		_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Start), "\t")
		if len(gantt)-1 == i {
			_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Stop))
		}
	}
	_, _ = fmt.Fprintf(w, "\n\n")
}

func outputSchedule(w io.Writer, rows [][]string, wait, turnaround, throughput float64) {
	_, _ = fmt.Fprintln(w, "Schedule table")
	table := tablewriter.NewWriter(w)
	table.SetHeader([]string{"ID", "Priority", "Burst", "Arrival", "Wait", "Turnaround", "Exit"})
	table.AppendBulk(rows)
	table.SetFooter([]string{"", "", "", "",
		fmt.Sprintf("Average\n%.2f", wait),
		fmt.Sprintf("Average\n%.2f", turnaround),
		fmt.Sprintf("Throughput\n%.2f/t", throughput)})
	table.Render()
}

//endregion

//region Loading processes.

var ErrInvalidArgs = errors.New("invalid args")

func loadProcesses(r io.Reader) ([]Process, error) {
	rows, err := csv.NewReader(r).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("%w: reading CSV", err)
	}

	processes := make([]Process, len(rows))
	for i := range rows {
		processes[i].ProcessID = mustStrToInt(rows[i][0])
		processes[i].BurstDuration = mustStrToInt(rows[i][1])
		processes[i].ArrivalTime = mustStrToInt(rows[i][2])
		if len(rows[i]) == 4 {
			processes[i].Priority = mustStrToInt(rows[i][3])
		}
	}

	return processes, nil
}

func mustStrToInt(s string) int64 {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	return i
}

//endregion
