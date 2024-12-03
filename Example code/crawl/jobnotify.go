package main

import "time"

func worker(j int) {
	time.Sleep(time.Second * time.Duration(j))
}

func spawn(f func(int)) (chan int, chan string) {
	job := make(chan int)
	quit := make(chan string)
	go func() {
		for {
			select {
			case j := <-job:
				f(j)
			case <-quit:
				quit <- "ok"
				return
			}
		}
	}()
	return job, quit
}

func main() {
	job, quit := spawn(worker)
	println("spawn a worker goroutine")

	// Send a job to the worker
	job <- 3

	time.Sleep(5 * time.Second)

	// notify the child goroutine to exit
	println("notify the worker to exit...")
	quit <- "exit"

	timer := time.NewTimer(time.Second * 10)
	defer timer.Stop()
	select {
	case status := <-quit:
		println("worker done:", status)
	case <-timer.C:
		println("wait worker exit timeout")
	}
}
