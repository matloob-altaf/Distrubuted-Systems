// Implementation of a KeyValueServer. Students should write their code in this file.

package pa1

import (
	"DS_PA1/rpcs"
	"bufio"
	"bytes"
	"fmt"
	"net"
	"strconv"
)

const bufSize = 1024
const msgSize = 500

type keyValueServer struct {
	// TODO: implement this!
	listener net.Listener

	clients          map[net.Conn]client
	quit             bool
	clientActiveChan chan net.Conn
	clientDropChan   chan net.Conn
	clientsActive    int
}

// New creates and returns (but does not start) a new KeyValueServer.
func New() KeyValueServer {
	// TODO: implement this!
	// return nil

	server := keyValueServer{nil, make(map[net.Conn]client), false, make(chan net.Conn, 1), make(chan net.Conn, 1), 0}
	return &server
}

func (kvs *keyValueServer) StartModel1(port int) error {
	// TODO: implement this!
	// return nil

	ln, err := net.Listen("tcp", "localhost:"+strconv.Itoa(port))
	if err != nil {
		fmt.Println(err)
		return nil
	}
	kvs.listener = ln
	initDB()
	go acceptClients(kvs)
	go handleConnections(kvs)
	return err
}

func (kvs *keyValueServer) Close() {
	// TODO: implement this!
	kvs.quit = true
	for conn, client := range kvs.clients {
		close(client.keysQueue)
		removeClient(kvs, conn)
	}
	kvs.listener.Close()
	// for k := range kvstore {
	// 	clear(k)
	// }
}

func (kvs *keyValueServer) Count() int {
	// TODO: implement this!
	return kvs.clientsActive
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

type client struct {
	conn      net.Conn
	keysQueue chan string
}

func acceptClients(kvs *keyValueServer) {
	for {
		conn, err := kvs.listener.Accept()
		if kvs.quit == true {
			return
		}
		if err != nil {
			fmt.Println(err)
			return
		}
		kvs.clientActiveChan <- conn
	}
}

func handleConnections(kvs *keyValueServer) {
	for {
		select {
		case conn := <-kvs.clientActiveChan:
			kvs.clients[conn] = client{conn, make(chan string, msgSize)}
			kvs.clientsActive++
			go readFromClient(kvs, conn)
			go writeToClient(kvs, conn)
		case conn := <-kvs.clientDropChan:
			removeClient(kvs, conn)
		default:
			if kvs.quit == true {
				return
			}
		}
	}
}

func readFromClient(kvs *keyValueServer, conn net.Conn) {
	for {

		str, err := bufio.NewReader(conn).ReadString('\n')
		// fmt.Println(str)
		buf := []byte(str)
		if err != nil {
			kvs.clientDropChan <- conn
			return
		}
		// parse
		msg := bytes.Split(buf, []byte(","))

		// 0 for put, 1 for get

		op := 0 //set it of for put and then update if requested operation is get
		if string(msg[0]) == "get" {
			op = 1
		}
		processRequest(kvs, conn, msg, op)

	}
}

// all clients share one put channel, but can read simultaneously
// op: 0 for put, 1 for get
func processRequest(kvs *keyValueServer, conn net.Conn, msg [][]byte, op int) {

	key := string(bytes.TrimSuffix(msg[1][:], []byte("\n")))
	switch op {
	case 0:
		put(key, msg[2])
	case 1:
		if len(kvs.clients[conn].keysQueue) < 500 {

			kvs.clients[conn].keysQueue <- key
		}
	}
}

func writeToClient(kvs *keyValueServer, conn net.Conn) {
	// only stop when disconnected or kvs.clients[conn] closed
	key := <-kvs.clients[conn].keysQueue
	msg := append([]byte(key+","), get(key)...)
	for client := range kvs.clients {
		// fmt.Println(string(msg))
		// _, err := conn.Write(msg)
		_, err := client.Write(msg)
		if err != nil {
			if kvs.quit == false {
				kvs.clientDropChan <- client
			}
			return
		}
	}

}
func removeClient(kvs *keyValueServer, conn net.Conn) {
	conn.Close()
	delete(kvs.clients, conn)
	kvs.clientsActive--
}
