# go-raknet

go-raknet is a library that implements a basic version of the RakNet protocol, which is used for
Minecraft (Bedrock Edition). It implements Unreliable, Reliable and 
ReliableOrdered packets and sends user packets as ReliableOrdered.

go-raknet attempts to abstract away direct interaction with RakNet, and provides simple to use, idiomatic Go
API used to listen for connections or connect to servers.

## Getting started

### Prerequisites
**As of go-raknet version 1.14.0, go-raknet requires at least Go 1.22**. Version 1.12.1 of go-raknet is
the last version of the library that supports Go 1.18 and above.

### Usage
go-raknet can be used for both clients and servers, (and proxies, when combined) in a way very similar to the
standard net.TCP* functions.

Basic RakNet server:
```go
package main

import (
	"github.com/sandertv/go-raknet"
)

func main() {
    listener, _ := raknet.Listen("0.0.0.0:19132")
    defer listener.Close()
    for {
        conn, _ := listener.Accept()
        
        b := make([]byte, 1024*1024*4)
        _, _ = conn.Read(b)
        _, _ = conn.Write([]byte{1, 2, 3})
        
        conn.Close()
    }
}
```

Basic RakNet client:

```go
package main

import (
	"github.com/sandertv/go-raknet"
)

func main() {
    conn, _ := raknet.Dial("mco.mineplex.com:19132")
    defer conn.Close()
    
    b := make([]byte, 1024*1024*4)
    _, _ = conn.Write([]byte{1, 2, 3})
    _, _ = conn.Read(b)
}
```

### Sending behaviour
Data passed to `Write` is placed in a send queue with a fixed size limit. If the queue is already full,
`Write` blocks until earlier data has been sent out and space becomes available, so a fast writer is slowed
down by backpressure instead of using unbounded memory. A single packet larger than the queue limit is
rejected with an error.

Reliable data is not put on the wire as fast as possible. The connection keeps a congestion window that
limits how many bytes may be in flight without being acknowledged, and grows or shrinks it based on the ACKs
and NAKs coming back from the other side. Datagrams that are reported lost, or that go unacknowledged for
too long, are sent again using a timeout derived from the measured RTT.

Protocol control messages, such as connection maintenance, are sent from a separate queue that is served
first, so a large transfer cannot delay them.

### Documentation
[![PkgGoDev](https://pkg.go.dev/badge/github.com/sandertv/go-raknet)](https://pkg.go.dev/github.com/sandertv/go-raknet)

## Contact
[![Discord Banner 2](https://discordapp.com/api/guilds/623638955262345216/widget.png?style=banner2)](https://discord.gg/U4kFWHhTNR)
