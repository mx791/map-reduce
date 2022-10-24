package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
)

const (
	StartMessage      = "start-split"
	MapMessage        = "map"
	ShuffleMessage    = "shuffle"
	TaskReset         = "task-reset"
	ReduceMsg         = "task-reduce"
	AddRunnginTask    = "add-running-task"
	RemoveRunnginTask = "remove-running-task"
	FileBufferSize    = 1024 * 1024 * 2
)

func CreateServer(handler MapReducer) Server {
	path := "../files/nodes.json"
	return Server{0, LoadWorkerList(path), 0, handler}
}

// parse la liste des noeuds depuis un fichier JSON
func LoadWorkerList(path string) []string {
	b, err := os.ReadFile(path)
	if err != nil {
		fmt.Println("Impossible de lire le fichier...")
		return make([]string, 0)
	}
	nodes := make([]string, 0)
	json.Unmarshal(b, &nodes)
	fmt.Println("INIT", "Liste des noeuds chargés ("+strconv.FormatInt(int64(len(nodes)), 10)+" noeuds)")
	return nodes
}

var counter = 0
var reduceCount = 0

type Server struct {
	runningTaskCounter int
	serverList         []string
	reduceCount        int
	handler            MapReducer
}

func (x Server) Start(port string) {
	tcpAddr, _ := net.ResolveTCPAddr("tcp4", port)
	listener, _ := net.ListenTCP("tcp", tcpAddr)
	fmt.Println("listening on port " + port)
	fmt.Println("INIT Buffer size : ", FileBufferSize)
	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go x.MainHandler(conn)
	}
}

func (x Server) MainHandler(conn net.Conn) {
	buf := make([]byte, FileBufferSize)
	n, _ := conn.Read(buf)
	messages := make([]string, 0)
	err := json.Unmarshal(buf[:n], &messages)

	if err != nil {
		fmt.Println("erreur reception")
		conn.Close()
		return
	}
	if messages[0] == MapMessage {
		x.StartTask(messages)

	} else if messages[0] == AddRunnginTask {
		counter += 1
	} else if messages[0] == StartMessage {
		x.SplitHandler(messages)

	} else if messages[0] == RemoveRunnginTask {
		counter += -1
		if counter == 0 {
			// on declenche le reduce
			fmt.Println("SHUFFLE", "fin du Shuffle")
			x.SendReduceData(x.handler.GetReduceData())
		}

	} else if messages[0] == ShuffleMessage {
		fmt.Println("SHUFFLE", "reception de données de shuffle")
		x.handler.ShuffleReceiver(messages)

	} else if messages[0] == ReduceMsg {
		x.handler.ReduceReceiver(messages)
		reduceCount += 1
		if reduceCount == len(x.serverList) {
			x.handler.ReduceDone()
		}
	}
	conn.Close()
}

func (x Server) SplitHandler(messages []string) {
	subs := x.handler.Split(messages, len(x.serverList))
	for id := 0; id < len(subs); id++ {
		connection, _ := net.Dial("tcp", x.serverList[id])
		sendData := append([]string{MapMessage}, subs[id]...)
		str, _ := json.Marshal(sendData)
		connection.Write(str)
		connection.Close()
	}
}

// envoi un message à tout le monde
func (x Server) BroadcastMessage(message []string) {
	for node := range x.serverList {
		connection, _ := net.Dial("tcp", x.serverList[node])
		str, _ := json.Marshal(message)
		connection.Write(str)
		connection.Close()
	}
}

func (x Server) StartTask(messages []string) {
	fmt.Println("JOB", "Starting new Task")
	reduceCount = 0
	x.BroadcastMessage([]string{AddRunnginTask})

	// Map
	fmt.Println("MAP", "début du Map")
	hashes, transferData := x.handler.Map(messages, len(x.serverList))
	fmt.Println("MAP", "fin du Map")

	// Shuffle
	fmt.Println("SHUFFLE", "début du Shuffle")
	for id := 0; id < len(hashes); id++ {
		targetServer := x.serverList[hashes[id]%len(x.serverList)]
		connection, err := net.Dial("tcp", targetServer)
		if err == nil {
			data := []string{ShuffleMessage, transferData[id]}
			str, _ := json.Marshal(data)
			connection.Write([]byte(str))
			connection.Close()
		}
	}
	x.BroadcastMessage([]string{RemoveRunnginTask})
}

// envoie au premier noeud des données locales
func (x Server) SendReduceData(data string) {
	connection, _ := net.Dial("tcp", x.serverList[0])
	full_str, _ := json.Marshal([]string{ReduceMsg, data})
	connection.Write(full_str)
	connection.Close()
}
