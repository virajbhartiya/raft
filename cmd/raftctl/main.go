package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/rpc"
	"os"
)

func main() {
	var (
		address = flag.String("address", "localhost:8080", "RPC address")
		command = flag.String("command", "", "Command: state, leader, start")
		key     = flag.String("key", "", "Key for start command")
		value   = flag.String("value", "", "Value for start command")
	)
	flag.Parse()

	if *command == "" {
		fmt.Fprintf(os.Stderr, "Error: -command is required\n")
		os.Exit(1)
	}

	client, err := rpc.Dial("tcp", *address)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error connecting to server: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()

	switch *command {
	case "state":
		var state map[string]interface{}
		err := client.Call("Raft.GetState", struct{}{}, &state)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		json, _ := json.MarshalIndent(state, "", "  ")
		fmt.Println(string(json))
	case "leader":
		var leader string
		err := client.Call("Raft.GetLeader", struct{}{}, &leader)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		fmt.Println(leader)
	case "start":
		if *key == "" {
			fmt.Fprintf(os.Stderr, "Error: -key is required for start command\n")
			os.Exit(1)
		}
		var result struct {
			Index uint64
			Term  uint64
			OK    bool
		}
		cmd := map[string]string{
			"op":    "set",
			"key":   *key,
			"value": *value,
		}
		cmdBytes, _ := json.Marshal(cmd)
		err := client.Call("Raft.Start", cmdBytes, &result)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		if result.OK {
			fmt.Printf("Command started: index=%d term=%d\n", result.Index, result.Term)
		} else {
			fmt.Println("Not leader")
		}
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", *command)
		os.Exit(1)
	}
}

