package main

import (
	"fmt"
	"net/rpc"
)

type Argsc struct {
	A, B int
}

func main() {
	// 连接到RPC服务
	client, err := rpc.Dial("tcp", "127.0.0.1:12345")
	if err != nil {
		fmt.Println("Error connecting to server:", err)
		return
	}
	defer client.Close()

	// 准备参数
	args := &Argsc{A: 5, B: 3}

	// 调用远程方法
	var result int
	err = client.Call("Arith.Multiply", args, &result)
	if err != nil {
		fmt.Println("Error calling remote method:", err)
		return
	}

	// 打印结果
	fmt.Printf("Result: %d\n", result)
}
