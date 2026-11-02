package main

import (
	"log"
	"os"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Node struct {
	nodeId int64
	clock  string
	state  string
	port   int
	mu     sync.Mutex
	port   map[int]int
}

type Approval struct {
	isApproved bool
}

func (n *Node) tick(receivedClock int64) int64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.clock = max(n.clock, receivedClock) + 1
	return n.clock
}

func (n *Node) Request(port int, node Node) {

}

func (n *Node) Enter(accessor Node) {
	state = "wanted"
	for _, p := range ports {
		go request(p, n)
	}
}

func main() {
	// pick the port when starting: ex. go run . 1
	if len(os.Args) < 2 {
		log.Fatal("usage: go run . <port>")
	}
	port := os.Args[1]

	// Connect to own port
	url := "http:localhost:" + port
	conn, err := grpc.NewClient(url, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("event=CONNECT_FAILED")
	}
	defer conn.close()

	for i := 1; i <= 3; i++ {
		if i = port 
	}

	node := pb.NewChitChatServiceClient(conn)
	n := &Node{
		port: port,
		state: "released"
	}

	n.mu.Lock()
	n.clock++
	ts := n.clock
	n.mu.Unlock()
}

// package main

// import (
// 	"bufio"
// 	"context"
// 	"fmt"
// 	"log"
// 	"os"
// 	"os/signal"
// 	"sync"
// 	"syscall"
// 	"uuid"

// 	"google.golang.org/grpc"
// 	"google.golang.org/grpc/credentials/insecure"
// 	"google.golang.org/grpc/status"

// 	pb "chitchat/grpc"
// )

// type Client struct {
// 	name    string
// 	clock   int64
// 	mu      sync.Mutex
// 	service pb.ChitChatServiceClient
// }

// // receives messages from the server until the stream closes
// func (c *Client) receive(stream pb.ChitChatService_ConnectClient) {
// 	for {
// 		msg, err := stream.Recv()
// 		if err != nil {
// 			log.Printf("event=DISCONNECT client=%s reason=%q", c.name, err.Error())
// 			fmt.Println("disconnected:", err)
// 			return
// 		}

// 		// one tick per received message; show the server's logical time (the L in the text)
// 		clock := c.tick(msg.Timestamp)
// 		fmt.Printf("[%s @ %d] %s\n", msg.Sender, msg.Timestamp, msg.Content)
// 		log.Printf("event=RECEIVE client=%s from=%s lamport=%d clock=%d msg=%q", c.name, msg.Sender, msg.Timestamp, clock, msg.Content)
// 	}
// }

// func (c *Client) send(text string) {
// 	c.mu.Lock()
// 	c.clock++
// 	ts := c.clock
// 	c.mu.Unlock()

// 	log.Printf("event=SEND client=%s lamport=%d msg=%q", c.name, ts, text)
// 	_, err := c.service.PostMessage(context.Background(), &pb.Message{
// 		Sender:    c.name,
// 		Content:   text,
// 		Timestamp: ts,
// 	})
// 	if err != nil {
// 		log.Printf("event=SEND_FAILED client=%s err=%v", c.name, err)
// 		fmt.Println("message rejected:", status.Convert(err).Message())
// 	}
// }

// func main() {
// 	// pick the username when starting: ex. go run . alice
// 	if len(os.Args) < 2 {
// 		log.Fatal("usage: go run . <username>")
// 	}
// 	name := os.Args[1]

// 	f, err := os.OpenFile("client_"+name+"_"+uuid.NewV4().String()+".log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
// 	if err != nil {
// 		log.Fatalf("could not open log file: %v", err)
// 	}
// 	defer f.Close()

// 	log.SetOutput(f)
// 	log.SetPrefix("[Client] ")
// 	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

// 	url := "localhost:5050"
// 	conn, err := grpc.NewClient(url,
// 		grpc.WithTransportCredentials(insecure.NewCredentials()))
// 	if err != nil {
// 		log.Fatalf("event=CONNECT_FAILED client=%s addr=%s err=%v", name, url, err)
// 	}
// 	defer conn.Close()

// 	client := pb.NewChitChatServiceClient(conn)
// 	c := &Client{name: name, service: client}

// 	c.mu.Lock()
// 	c.clock++
// 	ts := c.clock
// 	c.mu.Unlock()

// 	stream, err := client.Connect(context.Background(), &pb.User{Username: name, Timestamp: ts})
// 	if err != nil {
// 		log.Fatalf("event=CONNECT_FAILED client=%s addr=%s err=%v", name, url, err)
// 	}
// 	log.Printf("event=CONNECT client=%s addr=%s lamport=%d", name, url, ts)

// 	go c.receive(stream) // once receive is a method too

// 	// Ctrl+C / SIGTERM: log the disconnect, then exit (closing the connection ends the stream)
// 	sig := make(chan os.Signal, 1)
// 	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
// 	go func() {
// 		<-sig
// 		log.Printf("event=DISCONNECT client=%s reason=%q", name, "interrupted")
// 		os.Exit(0)
// 	}()

// 	scanner := bufio.NewScanner(os.Stdin)
// 	for scanner.Scan() {
// 		c.send(scanner.Text())
// 	}
// 	log.Printf("event=DISCONNECT client=%s reason=%q", name, "stdin closed")
// }
