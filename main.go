//
// main.go
// Copyright (C) 2017 yanming02 <yanming02@baidu.com>
//
// Distributed under terms of the MIT license.
//

package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
)

var (
	ClientsCh chan int
	quit      chan bool

	concur   int
	clients  int
	filename string
	filenum  int
	addrs    string
)

func init() {
	flag.IntVar(&concur, "cc", 200, "concurrency go routine")
	flag.IntVar(&clients, "c", 20, "clients number")
	flag.StringVar(&filename, "file", "keys", "filename prefix to load keys from")
	flag.IntVar(&filenum, "filenum", 200, "file number. prefix.[0-number]. eg:keys.0, keys.1")
	flag.StringVar(&addrs, "addrs", "", "server list. eg: host:ip,host:ip,host:ip")
}
func loadKeys(path string) string {
	fi, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer fi.Close()
	keys, err := ioutil.ReadAll(fi)
	// fmt.Println(string(fd))
	return string(keys)
}

func writeAfterRead(mc *memcache.Client, key string, concurCh chan int) error {
	startRead := time.Now()
	item, err := mc.Get(key)
	if err != nil {
		<-concurCh
		return err
	}
	endRead := time.Now()
	startWrite := time.Now()
	mc.Set(item)
	endWrite := time.Now()
	log.Printf("client read cost: %s write cost: %s", endRead.Sub(startRead), endWrite.Sub(startWrite))
	<-concurCh
	if len(concurCh) == 0 {
		//cquit <- true
		fmt.Println("Mark client to quit")
	}
	return nil
}

func runClient(server string, idx int, concurCh chan int) {
	fmt.Printf("Run client[%d] to connect to server %s\n", idx, server)

	mc := memcache.New(server)

	source := rand.NewSource(time.Now().Unix() + int64(idx))
	r := rand.New(source)
	postfix := fmt.Sprintf("%d", r.Intn(filenum))

	file := filename + "." + postfix
	fmt.Printf("Loading keys from file %s\n", file)
	keys := loadKeys(file)

	lines := strings.Split(keys, "\n")
	fmt.Println("Loading complete, run test")

	for _, k := range lines {
		concurCh <- 1
		go writeAfterRead(mc, k, concurCh)
	}
	//<-cquit
	fmt.Printf("Clinet[%d] run test done\n", idx)
	<-ClientsCh
	if len(ClientsCh) == 0 {
		quit <- true
		fmt.Println("Mark Benchmark to quit")
	}
}

func main() {
	flag.Parse()
	quit = make(chan bool, 1)
	ClientsCh = make(chan int, clients)
	servers := strings.Split(addrs, ",")
	serverIdx := 0
	clientIdx := 0

	fmt.Println("Benchmark starting...")

	for clientIdx < clients {
		ClientsCh <- 1
		concurCh := make(chan int, concur)
		go runClient(servers[serverIdx], clientIdx, concurCh)
		serverIdx = (serverIdx + 1) % len(servers)
		clientIdx = clientIdx + 1
	}
	<-quit
}
