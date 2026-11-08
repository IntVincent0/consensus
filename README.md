# Distributed Systems - Mandatory Assignment 4

Distributed mutual exclusion between peer nodes, using the Ricart-Agrawala algorithm with Lamport clocks. Written in Go. Nodes talk to each other over gRPC.

## How to run this project

Run these three commands in three separate terminals from the root folder.

```bash
go run ./node -port 5001 -peers 5002,5003
```
```bash
go run ./node -port 5002 -peers 5001,5003
```
```bash
go run ./node -port 5003 -peers 5001,5002
```

-port is the port this node listens on.
-peers is a comma-separated list of the ports of all the other nodes. Don't use spaces.

The algorithm assumes all nodes are running and know each other's port numbers, which is why every command lists the other two. Each node waits until it can reach all of its peers, so start the three nodes within a few seconds of each other.

### Output

All output is written to `node.log` in the project root. You will see each node request access, defer its reply to other nodes, and enter and exit the critical section one at a time.
