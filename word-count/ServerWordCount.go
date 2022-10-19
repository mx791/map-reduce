package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
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

var runningTaskCounter = 0
var serverList = []string{}
var reduceCount = 0

func main() {
	port := ":" + os.Args[1]
	tcpAddr, _ := net.ResolveTCPAddr("tcp4", port)
	listener, _ := net.ListenTCP("tcp", tcpAddr)
	fmt.Println("listening on port " + port)
	fmt.Println("INIT Buffer size : ", FileBufferSize)
	serverList = LoadWorkerList("./worder.json")
	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go MainHandler(conn)
	}
}

func Log(channel string, msg string) {
	fmt.Println(channel, msg)
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
	Log("INIT", "Liste des noeuds chargés ("+strconv.FormatInt(int64(len(nodes)), 10)+" noeuds)")
	return nodes
}

// Parse et traite les données d'une connexion
func MainHandler(conn net.Conn) {
	buf := make([]byte, FileBufferSize)
	n, _ := conn.Read(buf)
	messages := make([]string, 0)
	json.Unmarshal(buf[:n], &messages)

	if messages[0] == MapMessage {
		StartTask(messages)

	} else if messages[0] == AddRunnginTask {
		runningTaskCounter++

	} else if messages[0] == StartMessage {
		SplitHandler(messages)

	} else if messages[0] == RemoveRunnginTask {
		runningTaskCounter--
		if runningTaskCounter == 0 {
			// on declenche le reduce
			Log("SHUFFLE", "fin du Shuffle")
			SendReduceData(GetReduceData())
		}

	} else if messages[0] == ShuffleMessage {
		ShuffleReceiver(messages)

	} else if messages[0] == ReduceMsg {
		reduce_mutex.Lock()
		ReduceReceiver(messages)
		reduceCount++
		if reduceCount == len(serverList) {
			ReduceDone()
		}
		reduce_mutex.Unlock()
	}
	conn.Close()
}

func SplitHandler(messages []string) {
	subs := Split(messages)
	for id := 0; id < len(subs); id++ {
		connection, _ := net.Dial("tcp", serverList[id])
		sendData := append([]string{MapMessage}, subs[id]...)
		str, _ := json.Marshal(sendData)
		connection.Write(str)
		connection.Close()
	}
}

// envoi un message à tout le monde
func BroadcastMessage(message []string) {
	for node := range serverList {
		connection, _ := net.Dial("tcp", serverList[node])
		str, _ := json.Marshal(message)
		connection.Write(str)
		connection.Close()
	}
}

func StartTask(messages []string) {
	Log("JOB", "Starting new Task")
	runningTaskCounter = 0
	reduceCount = 0
	BroadcastMessage([]string{AddRunnginTask})

	// Map
	Log("MAP", "début du Map")
	hashes, transferData := Map(messages)
	Log("MAP", "fin du Map")

	// Shuffle
	Log("SHUFFLE", "début du Shuffle")
	for id := 0; id < len(hashes); id++ {
		targetServer := serverList[hashes[id]%len(serverList)]
		connection, err := net.Dial("tcp", targetServer)
		if err == nil {
			data := []string{ShuffleMessage}
			data = append(data, transferData[id]...)
			str, _ := json.Marshal(data)
			connection.Write([]byte(str))
			connection.Close()
		}
	}
	BroadcastMessage([]string{RemoveRunnginTask})
}

// envoie au premier noeud des données locales
func SendReduceData(data string) {
	connection, _ := net.Dial("tcp", serverList[0])
	full_str, _ := json.Marshal([]string{ReduceMsg, data})
	connection.Write(full_str)
	connection.Close()
}

// sépare le texte par lignes
func Split(messages []string) [][]string {
	text := messages[1]
	lines := strings.Split(text, "\r")
	Log("SPLIT", strconv.FormatInt(int64(len(lines)), 10)+" lignes")
	stepp := len(lines) / len(serverList)

	linesSplited := make([][]string, len(serverList))
	for id := 0; id < len(serverList); id += 1 {
		txt := ""
		for id1 := 0; id1 < stepp; id1++ {
			txt += lines[id*stepp+id1]
		}
		linesSplited[id] = []string{txt}
	}

	return linesSplited
}

// fonction de MAP
// renvoie toutes les valeurs du shuffle, ainsi que leur hash
func Map(messages []string) ([]int, [][]string) {
	text := messages[1]
	re := regexp.MustCompile("[a-zàâçéèêëîïôûùüÿñæœ]+")
	matchs := re.FindAllString(strings.ToLower(text), -1)
	words := make(map[string]int)

	Log("MAP", "map sur "+strconv.FormatInt(int64(len(matchs)), 10)+" mots")

	// map
	for matchIndex := range matchs {
		currentWord := matchs[matchIndex]
		if _, ok := words[currentWord]; ok {
			words[currentWord] += 1
		} else {
			words[currentWord] = 1
		}
	}

	hashs := make([]int, 0)
	data := make([][]string, 0)

	for word := range words {
		hashs = append(hashs, HashWord(word))
		data = append(data, []string{word, strconv.FormatInt(int64(words[word]), 10)})
	}

	return hashs, data
}

func HashWord(word string) int {
	hash := 1
	for letter := range word {
		hash += int(word[letter])
	}
	return hash
}

var shuffle_datas = make(map[string]int)
var shuffleMutex = &sync.RWMutex{}

func ExtractInt(text string) int {
	re := regexp.MustCompile(`[0-9]+`)
	val, err := strconv.Atoi(re.FindString(text))
	if err != nil {
		return 0
	}
	return val
}

// reception de données de shuffle
func ShuffleReceiver(messages []string) {
	word := messages[1]
	value := messages[2]
	valueInt := ExtractInt(value)
	word = regexp.MustCompile("[a-zàâçéèêëîïôûùüÿñæœ]+").FindString(word)
	shuffleMutex.Lock()
	if _, ok := shuffle_datas[word]; ok {
		shuffle_datas[word] += valueInt
	} else {
		shuffle_datas[word] = valueInt
	}
	shuffleMutex.Unlock()
}

// renvoie les données du shuffle, pour que un noeud les aggrègent (reduce)
func GetReduceData() string {
	map_string, _ := json.Marshal(shuffle_datas)
	return string(map_string)
}

var reduce_datas = make(map[string]int)
var reduce_mutex = &sync.RWMutex{}

// receptionne et aggrège les données de tout le monde
func ReduceReceiver(messages []string) {
	words := make(map[string]int)
	json.Unmarshal([]byte(messages[1]), &words)
	for word := range words {
		reduce_datas[word] = words[word]
	}
}

// appelée quand tout est bon
func ReduceDone() {
	fmt.Println("Reduce, JOB DONE !!!")
	str, _ := json.Marshal(reduce_datas)
	os.WriteFile("./out.json", str, 1)
}
