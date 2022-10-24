package main

import "os"

func main() {

	port := ":" + os.Args[1]
	//port := ":1234"
	server := CreateServer(CreateWordCountHandler())
	server.Start(port)
}
