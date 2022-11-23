package main

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

var shuffleDatas = make(map[string]int)
var reduceDatas = make(map[string]int)
var shuffleMutex = &sync.RWMutex{}
var reduceMutex = &sync.RWMutex{}

type WordCountHandler struct{}

func CreateWordCountHandler() WordCountHandler {
	return WordCountHandler{}
}

func (x WordCountHandler) Reset() {
	shuffleDatas = make(map[string]int)
	reduceDatas = make(map[string]int)
	reduceMutex = &sync.RWMutex{}
	shuffleMutex = &sync.RWMutex{}
}

func (x WordCountHandler) HashWord(word string) int {
	hash := 1
	for letter := range word {
		hash += int(word[letter])
	}
	return hash
}

// reception de données de shuffle
func (x WordCountHandler) ShuffleReceiver(messages []string) {
	words := make(map[string]int)
	json.Unmarshal([]byte(messages[1]), &words)
	shuffleMutex.Lock()
	for word := range words {
		if _, ok := shuffleDatas[word]; ok {
			shuffleDatas[word] += words[word]
		} else {
			shuffleDatas[word] = words[word]
		}
	}
	shuffleMutex.Unlock()
}

// renvoie les données du shuffle, pour que un noeud les aggrègent (reduce)
func (x WordCountHandler) GetReduceData() string {
	shuffleMutex.Lock()
	fmt.Println(len(shuffleDatas), "mots géré par le noeud")
	map_string, _ := json.Marshal(shuffleDatas)
	shuffleMutex.Unlock()
	return string(map_string)
}

// receptionne et aggrège les données de tout le monde
func (x WordCountHandler) ReduceReceiver(messages []string) {
	words := make(map[string]int)
	json.Unmarshal([]byte(messages[1]), &words)
	reduceMutex.Lock()
	for word := range words {
		reduceDatas[word] = words[word]
	}
	reduceMutex.Unlock()
}

// appelée quand tout est bon
func (x WordCountHandler) ReduceDone() {
	fmt.Println("Reduce, JOB DONE !!!")
	reduceMutex.Lock()
	str, _ := json.Marshal(reduceDatas)
	reduceMutex.Unlock()
	os.WriteFile("./out.json", str, 0755)
}

// fonction de MAP
// renvoie toutes les valeurs du shuffle, ainsi que leur hash
func (x WordCountHandler) Map(messages []string, serverCount int) ([]int, []string) {
	text := messages[1]
	re := regexp.MustCompile("[a-zàâçéèêëîïôûùüÿñæœ]+")
	matchs := re.FindAllString(strings.ToLower(text), -1)
	words := make(map[string]int)

	fmt.Println("MAP", "map sur "+strconv.FormatInt(int64(len(matchs)), 10)+" mots")

	// map
	for matchIndex := range matchs {
		currentWord := matchs[matchIndex]
		if _, ok := words[currentWord]; ok {
			words[currentWord] += 1
		} else {
			words[currentWord] = 1
		}
	}

	serverDico := make([]map[string]int, serverCount)
	for i := 0; i < serverCount; i++ {
		serverDico[i] = make(map[string]int)
	}
	for word := range words {
		serverDico[x.HashWord(word)%serverCount][word] = words[word]
	}

	hashs := make([]int, len(serverDico))
	data := make([]string, len(serverDico))

	for i := 0; i < len(serverDico); i++ {
		hashs[i] = i
		map_string, _ := json.Marshal(serverDico[i])
		data[i] = string(map_string)
	}

	return hashs, data
}

// sépare le texte par lignes
func (x WordCountHandler) Split(messages []string, serverCount int) [][]string {
	text := messages[1]
	fmt.Println(len(text))
	lines := strings.Split(text, "\n")
	fmt.Println("SPLIT", strconv.FormatInt(int64(len(lines)), 10)+" lignes")
	stepp := len(lines) / serverCount

	linesSplited := make([][]string, serverCount)
	for id := 0; id < serverCount; id += 1 {
		var txt strings.Builder
		for id1 := 0; id1 < stepp; id1++ {
			txt.WriteString(lines[id*stepp+id1])
		}
		linesSplited[id] = []string{txt.String()}
	}

	return linesSplited
}
