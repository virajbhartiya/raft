package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/virajbhartiya/raft/pkg/fsm"
	"github.com/virajbhartiya/raft/pkg/raft"
	"github.com/virajbhartiya/raft/pkg/storage"
	"github.com/virajbhartiya/raft/pkg/transport"
)

func main() {
	var (
		id       = flag.String("id", "", "Node ID")
		dataDir  = flag.String("data-dir", "", "Data directory")
		peers    = flag.String("peers", "", "Comma-separated list of peer IDs")
		address  = flag.String("address", ":8080", "RPC address")
	)
	flag.Parse()

	if *id == "" {
		fmt.Fprintf(os.Stderr, "Error: -id is required\n")
		os.Exit(1)
	}
	if *dataDir == "" {
		fmt.Fprintf(os.Stderr, "Error: -data-dir is required\n")
		os.Exit(1)
	}

	store, err := storage.NewStorage(*dataDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating storage: %v\n", err)
		os.Exit(1)
	}

	applyCh := make(chan raft.ApplyMsg, 100)
	fsm := fsm.NewKVStore()

	peerList := []string{}
	if *peers != "" {
		for _, peer := range strings.Split(*peers, ",") {
			peerList = append(peerList, strings.TrimSpace(peer))
		}
	}

	rpcTransport := transport.NewRPCTransport(*address)
	if err := rpcTransport.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "Error starting transport: %v\n", err)
		os.Exit(1)
	}

	server := raft.NewServer(*id, peerList, store, rpcTransport, applyCh)
	server.Start()

	rpcTransport.RegisterHandler("GetState", func(args interface{}) (interface{}, error) {
		return server.GetState(), nil
	})
	rpcTransport.RegisterHandler("GetLeader", func(args interface{}) (interface{}, error) {
		return server.GetLeader(), nil
	})
	rpcTransport.RegisterHandler("Start", func(args interface{}) (interface{}, error) {
		cmdBytes := args.([]byte)
		index, term, ok := server.StartCommand(cmdBytes)
		return map[string]interface{}{
			"Index": index,
			"Term":  term,
			"OK":    ok,
		}, nil
	})

	fmt.Printf("Raft node %s started on %s with peers: %v\n", *id, *address, peerList)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for msg := range applyCh {
			if msg.CommandValid {
				fsm.Apply(msg.Command)
			} else if msg.SnapshotValid {
				fsm.Restore(msg.Snapshot)
			}
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println("\nShutting down...")
	server.Stop()
	rpcTransport.Stop()
	close(applyCh)
	
	select {
	case <-done:
		fmt.Println("Apply goroutine exited")
	case <-time.After(500 * time.Millisecond):
		fmt.Println("Warning: apply goroutine did not exit in time, continuing...")
	}
	
	store.Close()
	fmt.Println("Shutdown complete")
	os.Exit(0)
}

