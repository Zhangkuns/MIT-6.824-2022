package main

import "time"
import "math/rand"

func main() {
	rand.Seed(time.Now().UnixNano())
	count := 0
	finished := 0
	// Create a channel for tunneling updates to votes.
	/*voteChan := make(chan struct {
		count    *int
		finished *int
	})*/

	for i := 0; i < 10; i++ {
		go func(i int) {
			vote := requestVote1()
			if vote {
				count++
			}
			finished++
			count = i
			println("count: ", count)
		}(i)
	}

	for count < 5 && finished != 10 {
		// wait
	}
	if count >= 5 {
		println("received 5+ votes!")
	} else {
		println("lost")
	}
}

func requestVote1() bool {
	time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	return rand.Int()%2 == 0
}
