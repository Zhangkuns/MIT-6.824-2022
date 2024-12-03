package main

import (
	"fmt"
)

type T struct {
	Message string
}

/*func spawn(f func(chan T)) chan T {
	c := make(chan T)
	go func() {
		f(c)
		close(c) // Close the channel when the function is done
	}()

	return c
}*/

func main() {
	c := make(chan T)
	/*c := spawn(func(c chan T) {
		// Simulate some work
		for i := 0; i < 5; i++ {
			c <- T{Message: fmt.Sprintf("Message %d", i)}
		}
	})*/
	go func() {
		for i := 0; i < 5; i++ {
			c <- T{Message: fmt.Sprintf("Message %d", i)}
		}
		close(c)
	}()
	// Receive messages from the channel
	for msg := range c {
		fmt.Println(msg.Message)
	}
}
