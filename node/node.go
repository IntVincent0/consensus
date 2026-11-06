package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "consensus/grpc"
)

type Node struct {
	pb.UnimplementedChitChatServiceServer

	clock    int64
	reqClock int64
	state    string
	port     int
	mu       sync.Mutex
	peers    map[int]pb.ChitChatServiceClient
	deferred []chan struct{}
}

func (n *Node) tick(receivedClock int64) int64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.clock = max(n.clock, receivedClock) + 1
	return n.clock
}

func (n *Node) Enter() {
	n.mu.Lock()
	n.state = "wanted"
	n.clock++
	n.reqClock = n.clock
	req := &pb.AccessRequest{Port: int64(n.port), Clock: n.reqClock}
	n.mu.Unlock()

	log.Printf("[%d] requesting access at lamport clock=%d\n", n.port, n.reqClock)

	var wg sync.WaitGroup

	for port, service := range n.peers {
		wg.Add(1)
		go func(port int, service pb.ChitChatServiceClient) {
			defer wg.Done()

			reply, err := service.Request(context.Background(), req, grpc.WaitForReady(true))
			if err != nil {
				log.Printf("[%d] request to :%d failed: %v", n.port, port, err)
				return
			}
			n.tick(reply.Clock)
		}(port, service)
	}
	wg.Wait()

	n.mu.Lock()
	n.state = "held"
	n.mu.Unlock()
}

func (n *Node) Request(ctx context.Context, req *pb.AccessRequest) (*pb.Approval, error) {
	n.mu.Lock()
	n.clock = max(n.clock, req.Clock) + 1

	isPriority := n.reqClock < req.Clock ||
		(n.reqClock == req.Clock && int64(n.port) < req.Port)
	shouldDefer := n.state == "held" || (n.state == "wanted" && isPriority)

	if !shouldDefer {
		n.clock++
		time := n.clock
		n.mu.Unlock()
		return &pb.Approval{Clock: time}, nil
	}

	ch := make(chan struct{})
	n.deferred = append(n.deferred, ch)
	n.mu.Unlock()
	log.Printf("[%d] deferring reply to %d", n.port, req.Port)

	select {
	case <-ch:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	n.mu.Lock()
	n.clock++
	time := n.clock
	n.mu.Unlock()
	return &pb.Approval{Clock: time}, nil
}

func (n *Node) Exit() {
	n.mu.Lock()
	n.state = "released"
	for _, ch := range n.deferred {
		close(ch)
	}
	n.deferred = nil
	n.mu.Unlock()
	log.Printf("[%d] exited critical section", n.port)
}

// const (
// 	firstPort = 5000
// 	lastPort  = 5010
// )

// func findPort() (net.Listener, int) {
// 	for i := firstPort; i <= lastPort; i++ {
// 		ln, err := net.Listen("tcp", fmt.Sprintf(":%d", i))
// 		if err == nil {
// 			return ln, i
// 		}
// 	}
// 	log.Printf("No free port")
// 	return nil, 0
// }

func main() {
	port := flag.Int("port", 0, "port this node listens on")
	peerList := flag.String("peers", "", "comma-separated ports of the other nodes")
	flag.Parse()
	if *port == 0 || *peerList == "" {
		fmt.Fprintln(os.Stderr, "usage: node -port 5001 -peers 5002,5003")
		os.Exit(1)
	}

	// opens the log file and creates the template
	f, err := os.OpenFile("node.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("could not open log file: %v", err)
	}
	defer f.Close() // close it once done

	log.SetOutput(f)
	log.SetPrefix("[Server] ")
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	// Setup listener
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("could not listen %d: %v", *port, err)
	}
	node := &Node{
		port:  *port,
		peers: make(map[int]pb.ChitChatServiceClient),
	}
	log.Printf("Connected on port %d", *port)

	grpcServer := grpc.NewServer()
	pb.RegisterChitChatServiceServer(grpcServer, node)
	go grpcServer.Serve(listener)

	for _, s := range strings.Split(*peerList, ",") {
		p, err := strconv.Atoi(strings.TrimSpace(s))
		if err != nil {
			log.Fatalf("bad peer port %q: %v", s, err)
		}
		conn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", p),
			grpc.WithTransportCredentials(insecure.NewCredentials()))

		if err != nil {
			log.Fatalf("client for :%d: %v", p, err)
		}
		node.peers[p] = pb.NewChitChatServiceClient(conn)
	}

	for {
		time.Sleep(time.Duration(rand.Intn(3000)) * time.Millisecond)
		node.Enter()
		log.Printf("[%d] entered critical section", node.port)
		time.Sleep(time.Second)
		node.Exit()
	}
}
