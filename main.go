package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

type Entry struct {
	Value    string
	CreateAt time.Time
	TTL      time.Duration
}

var (
	dataMap = make(map[string]Entry)
	rwm     = sync.RWMutex{}
	wg      = sync.WaitGroup{}
)

func save(file *os.File) {
	rwm.RLock()
	bytes, err := msgpack.Marshal(dataMap)
	rwm.RUnlock()
	if err != nil {
		log.Println(err)
	}
	tmp := file.Name() + ".tmp"
	if err = os.WriteFile(tmp, bytes, 0644); err != nil {
		log.Println(err)
		return
	}
	if err = os.Rename(tmp, file.Name()); err != nil {
		log.Println(err)
		return
	}
}

func load(file *os.File) error {
	fileRead, err := os.ReadFile(file.Name())
	if err != nil {
		return err
	}
	if len(fileRead) == 0 {
		return nil
	}
	err = msgpack.Unmarshal(fileRead, &dataMap)
	if err != nil {
		return err
	}
	for key, value := range dataMap {
		if value.TTL > 0 && time.Since(value.CreateAt) > value.TTL {
			delete(dataMap, key)
		}
	}
	return nil
}

func handleConn(conn net.Conn) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, " ", 4)
		cmd := strings.ToUpper(parts[0])

		switch cmd {
		case "SET":
			if len(parts) < 3 {
				fmt.Fprintln(conn, "ERR usage: SET key value TTL")
				continue
			}
			ttl := time.Duration(0)
			if len(parts) == 4 {
				seconds, err := strconv.Atoi(parts[3])
				if err != nil {
					fmt.Fprintln(conn, "ERR invalid TTL")
					continue
				}
				ttl = time.Duration(seconds) * time.Second
			}
			rwm.Lock()
			dataMap[parts[1]] = Entry{parts[2], time.Now(), ttl}
			rwm.Unlock()
			fmt.Fprintln(conn, "OK")
		case "GET":
			if len(parts) < 2 {
				fmt.Fprintln(conn, "ERR usage: GET key")
				continue
			}
			rwm.RLock()
			value, ok := dataMap[parts[1]]
			rwm.RUnlock()
			if !ok {
				fmt.Fprintln(conn, "nil")
			} else if value.TTL > 0 && time.Since(value.CreateAt) > value.TTL {
				rwm.Lock()
				delete(dataMap, parts[1])
				rwm.Unlock()
				fmt.Fprintln(conn, "nil")
			} else {
				fmt.Fprintln(conn, value.Value)
			}
		case "DEL":
			if len(parts) < 2 {
				fmt.Fprintln(conn, "ERR usage: DEL key")
				continue
			}
			rwm.Lock()
			delete(dataMap, parts[1])
			rwm.Unlock()
			fmt.Fprintln(conn, "OK")
		default:
			fmt.Fprintln(conn, "ERR unknown command")
		}
	}
}
func main() {
	dataFile, err := os.OpenFile("data.msgpack", os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Println(err)
		return
	}
	defer dataFile.Close()
	err = load(dataFile)
	if err != nil {
		log.Println(err)
		return
	}
	if dataMap == nil {
		dataMap = make(map[string]Entry)
	}

	stopChan := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				save(dataFile)
			case <-stopChan:
				return
			}
		}
	}()

	listener, err := net.Listen("tcp", ":6379")
	if err != nil {
		log.Println(err)
		return
	}
	go func() {
		fmt.Println("Server started! Port: 6379")
		for {
			conn, err := listener.Accept()
			if err != nil {
				log.Printf("Server error: %v\n", err)
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				handleConn(conn)
			}()
		}
	}()
	shutdownChan := make(chan os.Signal, 1)
	signal.Notify(shutdownChan, os.Interrupt, syscall.SIGTERM)
	<-shutdownChan
	fmt.Println("\nStopping server, saving data...")
	listener.Close()
	close(stopChan)
	wg.Wait()
	save(dataFile)

}
