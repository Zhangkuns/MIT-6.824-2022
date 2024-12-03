package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
)

// Arith 包含了远程调用的方法
type Arith struct{}

type Args struct {
	A, B int
}

// Multiply 是一个远程调用的方法
func (a *Arith) Multiply(args *Args, reply *int) error {
	*reply = args.A * args.B
	return nil
}

func main() {
	// 创建一个Arith的实例
	arith := new(Arith)

	// 注册Arith实例
	errRegister := rpc.Register(arith)
	if errRegister != nil {
		log.Fatal("注册 RPC 服务失败:", errRegister)
	}

	// 创建一个TCP监听器
	listener, err := net.Listen("tcp", ":12345")
	if err != nil {
		fmt.Println("Error starting server:", err)
		return
	}
	defer listener.Close()

	fmt.Println("Server listening on :12345")

	for {
		// 接受客户端连接
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}

		// 在新的goroutine中为每个连接提供服务
		go rpc.ServeConn(conn)
	}
}
