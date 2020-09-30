// Implementation of a KeyValueServer. Students should write their code in this file.

package pa1

import (
	"DS_PA1/rpcs"
	"bufio"
	"fmt"
	"net"
	"strings"
)

// maximum size of buffer to maintain for slow connections
const msgBufferSize = 500

type keyValueServer struct {
	// TODO: implement this!
	listener        *net.TCPListener
	currentClientID int
	operationChan   chan int
	responseMsgChan chan []byte
	clients         chan map[int]*client
}

// New creates and returns (but does not start) a new KeyValueServer.
func New() KeyValueServer {
	// TODO: implement this!
	server := &keyValueServer{
		currentClientID: 0,
		operationChan:   make(chan int, 1),
		responseMsgChan: make(chan []byte, 1),
		clients:         make(chan map[int]*client, 1)}

	server.operationChan <- 1
	server.clients <- make(map[int]*client, 1)
	return server
}

func (kvs *keyValueServer) StartModel1(port int) error {
	// TODO: implement this!
	addr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}
	kvs.listener, err = net.ListenTCP("tcp", addr)
	if err != nil {
		return err
	}
	go kvs.acceptClients()
	go kvs.broadcastToClients()
	return nil
}

func (kvs *keyValueServer) Close() {
	// TODO: implement this!
	kvs.listener.Close()
	clients := <-kvs.clients
	for _, cl := range clients {
		cl.conn.Close()
		cl.restrictWrite <- 1
	}
	kvs.clients <- clients
	close(kvs.responseMsgChan)
}

func (kvs *keyValueServer) Count() int {
	// TODO: implement this!
	clients := <-kvs.clients
	count := len(clients)
	kvs.clients <- clients
	return count
}

func (kvs *keyValueServer) StartModel2(port int) error {
	// TODO: implement this!
	//
	// Do not forget to call rpcs.Wrap(...) on your kvs struct before
	// passing it to <sv>.Register(...)
	//
	// Wrap ensures that only the desired methods (RecvGet and RecvPut)
	// are available for RPC access. Other KeyValueServer functions
	// such as Close(), StartModel1(), etc. are forbidden for RPCs.
	//
	// Example: <sv>.Register(rpcs.Wrap(kvs))
	return nil
}

func (kvs *keyValueServer) RecvGet(args *rpcs.GetArgs, reply *rpcs.GetReply) error {
	// TODO: implement this!
	return nil
}

func (kvs *keyValueServer) RecvPut(args *rpcs.PutArgs, reply *rpcs.PutReply) error {
	// TODO: implement this!
	return nil
}

// TODO: add additional methods/functions below!
// TODO: add additional methods/functions below!
type client struct {
	id                int
	conn              *net.TCPConn
	restrictRead      chan int
	restrictWrite     chan int
	responseMsgBuffer chan []byte
	server            *keyValueServer
}

func (kvs *keyValueServer) acceptClients() {
	for {
		conn, err := kvs.listener.AcceptTCP()
		if err != nil {
			return
		}
		cl := &client{
			id:                kvs.currentClientID,
			conn:              conn,
			restrictWrite:     make(chan int, 1),
			restrictRead:      make(chan int, 1),
			responseMsgBuffer: make(chan []byte, msgBufferSize),
			server:            kvs}
		kvs.currentClientID++
		clients := <-kvs.clients
		clients[cl.id] = cl
		kvs.clients <- clients
		go cl.readFromClient()
		go cl.writeToClient()
	}
}
func (cl *client) readFromClient() {
	reader := bufio.NewReader(cl.conn)
	for {
		msg, err := reader.ReadBytes('\n')
		if err != nil {
			cl.restrictRead <- 1
			return
		}
		responseFlag, responseMsg := cl.server.parse(string(msg))
		switch responseFlag {
		case true:
			select {
			case cl.server.responseMsgChan <- []byte(responseMsg):
				break
			case <-cl.restrictWrite:
				cl.restrictRead <- 1
				return
			}
		case false:
			select {
			case <-cl.restrictWrite:
				cl.restrictRead <- 1
				return
			default:
				break
			}
		}
	}
}

func (cl *client) writeToClient() {
	for {
		select {
		case data, ok := <-cl.responseMsgBuffer:
			if !ok {
				return
			}
			_, err := cl.conn.Write(data)
			if err != nil {
				return
			}
		case <-cl.restrictRead:
			clients := <-cl.server.clients
			delete(clients, cl.id)
			cl.server.clients <- clients
			return
		}
	}
}

func (kvs *keyValueServer) broadcastToClients() {
	for {
		select {
		case data, ok := <-kvs.responseMsgChan:
			if !ok {
				return
			}
			clients := <-kvs.clients
			for _, cl := range clients {
				select {
				case cl.responseMsgBuffer <- data:
					break
				default:
					break
				}
			}
			kvs.clients <- clients
		}
	}
}

func (kvs *keyValueServer) parse(msg string) (bool, string) {
	requestMsg := strings.Split(msg, ",")
	switch strings.EqualFold(requestMsg[0], "put") {
	case true:
		requestMsg[2] = strings.TrimSpace(requestMsg[2])
		<-kvs.operationChan
		put(requestMsg[1], []byte(requestMsg[2]))
		kvs.operationChan <- 1
		return false, ""
	case false:
		requestMsg[1] = strings.TrimSpace(requestMsg[1])
		<-kvs.operationChan
		value := get(requestMsg[1])
		responseMsg := fmt.Sprintf("%v,%v\n", requestMsg[1], string(value))
		kvs.operationChan <- 1
		return true, responseMsg
	default:
		return false, ""
	}
}
