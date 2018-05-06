package main

import "os"

func main() {
	mode := os.Args[1]
	//path := "000000-025959/"
	path := "medium_test.csv"
	if mode == "client" {
		client, _ := InitClient()
		go client.Run(path)
		client.TCPListener()

	} else if mode == "worker" {
		worker, _ := InitWorker()

		go worker.PeerListener()
		worker.ClientListener()
	}
	// loop
}
