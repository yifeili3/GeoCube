package main

import "os"

func main() {
	mode := os.Args[1]
	path := "000000-025959/"

	if mode == "client" {
		client, _ := InitClient()
		client.Run(path)

	} else if mode == "worker" {
		worker, _ := InitWorker()

		clConnection := worker.ClientListener()
		peerConnection := worker.PeerListener()
		for {
			go worker.HandleClientRequests(<-clConnection)
			go worker.HandlePeerResults(<-peerConnection)
		}
	}
	// loop
}
