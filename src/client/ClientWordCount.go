package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
)

var server = "localhost:1234"

func main() {
	b, err := os.ReadFile("./DeLaTerreALaLune.txt")
	if err != nil {
		fmt.Println("Impossible de lire le fichier...")
		return
	}
	con, err1 := net.Dial("tcp", server)
	if err1 != nil {
		fmt.Println("Impossible de se connecter au serveur...")
		return
	}
	str, _ := json.Marshal([]string{"start-split", string(b)})
	// str, _ := json.Marshal([]string{"start-split", "un deux deux trois troise trois"})
	con.Write(str)
	con.Close()
	fmt.Println(len(str), "bytes de texte")

	// fmt.Println(string(str))
}
