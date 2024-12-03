package main

import (
	"fmt"
	"log"
	"net/rpc"
)

func main() {
	client, err := rpc.DialHTTP("unix", "/tmp/rpc.sock")
	if err != nil {
		log.Fatal("dialing:", err)
	}
	// Synchronous call
	name := "Joe"
	var reply string
	/*
	   1. 注册到HTTP中的类和函数名。
	   2. 调用函数对应的参数。
	*/
	err = client.Call("Greeter.Greet", &name, &reply)
	if err != nil {
		log.Fatal("greeter error:", err)
	}
	fmt.Printf("Got '%s'\n", reply)
}
