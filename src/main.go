/*
 * Main program for the DNS server and clerk
 */

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"MIT6.824/labrpc"
	"MIT6.824/raft"
	"MIT6.824/raftkv"
	"golang.org/x/net/dns/dnsmessage"
)

func my_run_client(cfg *raftkv.Config, me int, ca chan bool, addr_string string, fn func(me int, ck *raftkv.Clerk), context context.Context) {
	ok := false
	defer func() { ca <- ok }()
	ck := cfg.MakeClient(cfg.All())

	// Add the UDP connection handler for the Clerk
	serverAddr, err := net.ResolveUDPAddr("udp", addr_string)
	listen, err := net.ListenUDP("udp", serverAddr)
	if err != nil {
		fmt.Printf("*** listen failed error when creating clerk %v, with address %v, error:%v\n", me, addr_string, err)
		return
	}
	defer listen.Close() // Close the connection when the function returns
	fmt.Printf("UDP server (clerk %v) listening on %v...\n", me, addr_string)
	ck.Listen = listen

	// Add a sentinel context to the clerk
	ck.Context = context

	// Core routine for the clerk
	fn(me, ck)

	// finish jobs
	ok = true
	cfg.DeleteClient(ck)
}

func my_make_config(n int, unreliable bool, maxraftstate int) *raftkv.Config {
	raftkv.Ncpu_once.Do(func() {
		if runtime.NumCPU() < 2 {
			fmt.Printf("warning: only one CPU, which may conceal locking bugs\n")
		}
		rand.Seed(raftkv.MakeSeed())
	})
	runtime.GOMAXPROCS(4)
	cfg := &raftkv.Config{}
	cfg.T = &testing.T{}
	cfg.Net = labrpc.MakeNetwork() 
	cfg.N = n
	cfg.Kvservers = make([]*raftkv.KVServer, cfg.N)
	cfg.Saved = make([]*raft.Persister, cfg.N)
	cfg.Endnames = make([][]string, cfg.N)
	cfg.Clerks = make(map[*raftkv.Clerk][]string)
	cfg.NextClientId = cfg.N + 1000 // client ids start 1000 above the highest serverid
	cfg.Maxraftstate = maxraftstate
	cfg.Start = time.Now()

	// create a full set of KV servers.
	for i := 0; i < cfg.N; i++ {
		cfg.StartServer(i)
	}

	cfg.ConnectAll()

	cfg.Net.Reliable(!unreliable)

	return cfg
}

func Warn(me int, msgfmt string, args ...interface{}) {
	fmt.Printf("[Warn] - Clerk %d: ", me)
	fmt.Printf(msgfmt, args...)
}

func clerk_routine(me int, ck *raftkv.Clerk) {

	buffer := make([]byte, 1024)
	var clerk_put_count int32 = 0
	var clerk_query_count int32 = 0
	var clerk_received_count int32 = 0

	defer func() {
		atomic.AddInt32(&total_put, clerk_put_count)
		atomic.AddInt32(&total_query, clerk_query_count)
		atomic.AddInt32(&total_received, clerk_received_count)
	}()

	for {
		// in each loop, read data from UDP connection

		select {
		case <-ck.Context.Done():
			fmt.Printf("Clerk %v received done signal, exiting...\n", me)
			return
		default:
			ck.Listen.SetReadDeadline(time.Now().Add(1 * time.Second))
			n, addr, err := ck.Listen.ReadFromUDP(buffer)
			if err != nil {
				if ne, ok := err.(net.Error); ok && ne.Timeout() {
					continue
				}
				Warn(me, "Read data error:%v\n", err)
				return
			}
			// fmt.Printf("Clerk %v received data from %v: %v\n", me, addr, string(buffer[:n]))

			// Add total received by 1
			clerk_received_count++

			// parse the packet by dns protocol
			var msg dnsmessage.Message
			if err := msg.Unpack(buffer); err == nil {
				//
				// fmt.Println(msg)
				// ck.Put("hostname", "127.0.1.2")
				// ck.Put("hostname.", "127.0.1.3")

				for _, question := range msg.Questions {
					name := question.Name.String()
					name = name[:len(name)-1]
					if ck.Get(name) == "" {
						continue
					}
					ip_addr := net.ParseIP(ck.Get(name))
					var ip_addr_bytes [4]byte = [4]byte{ip_addr[12], ip_addr[13], ip_addr[14], ip_addr[15]}
					msg.Response = true
					msg.Answers = append(msg.Answers, dnsmessage.Resource{
						Header: dnsmessage.ResourceHeader{
							Name:  question.Name,
							Type:  question.Type,
							Class: question.Class,
							TTL:   60,
						},
						Body: &dnsmessage.AResource{A: ip_addr_bytes},
					})
				}

				packed, err := msg.Pack()
				if err != nil {
					fmt.Println(err)
					return
				}
				if _, err := ck.Listen.WriteToUDP(packed, addr); err != nil {
					fmt.Println(err)
				}

				continue
			}

			// If failed, parse the packet by our designed protocol
			packet := string(buffer[:n])

			// parse the packet
			var data map[string]interface{}
			json.Unmarshal([]byte(packet), &data)

			// get the "Type" field, which may not exist. If not exist, continue
			if _, ok := data["Type"]; !ok {
				Warn(me, "Received packet without Type field: %v\n", data)
				continue
			}

			switch data["Type"] {
			case "Put":
				// put data
				if _, ok := data["Key"]; !ok {
					Warn(me, "Received packet without Key field: %v\n", data)
					continue
				}
				if _, ok := data["Value"]; !ok {
					Warn(me, "Received packet without Value field: %v\n", data)
					continue
				}

				key := data["Key"].(string)
				value := data["Value"].(string)
				ck.Put(key, value)
				// fmt.Printf("Clerk %v put key:%v, value:%v\n", me, key, value)
				clerk_put_count++
			case "Query":
				// query data
				if _, ok := data["Key"]; !ok {
					Warn(me, "Received packet without Key field: %v\n", data)
					continue
				}
				key := data["Key"].(string)
				value := ck.Get(key)
				// fmt.Printf("Clerk %v query key:%v, value:%v\n", me, key, value)
				// prepare the response packet
				response := map[string]string{
					"Type":  "Response",
					"Key":   key,
					"Value": value,
				}
				responsePacket, err := json.Marshal(response)
				if err != nil {
					Warn(me, "Marshal response error:%v\n", err)
					return
				}
				// send the response packet
				_, err = ck.Listen.WriteToUDP(responsePacket, addr)
				if err != nil {
					Warn(me, "Send response error:%v\n", err)
					return
				}
				clerk_query_count++

			default:
				Warn(me, "Clerk %v received unknown packet: %v\n", me, data)
			}
		}
	}
}

func my_make_dns_address(ncli int) []string {
	res := make([]string, ncli)
	for i := 0; i < ncli; i++ {
		res[i] = fmt.Sprintf(":%d", 9876+i)
	}
	return res
}

var total_put int32 = 0      // total number of put requests
var total_query int32 = 0    // total number of query requests
var total_received int32 = 0 // total number of received responses (put+query+invalid packet)

func main() {
	// Parse the command line arguments
	var _nclerks int
	var _nservers int
	var _unreliable bool
	var _dnsDuration int
	var _crash bool

	// Set the default values
	flag.IntVar(&_nclerks, "nclerks", 5, "Number of dns clerks")
	flag.IntVar(&_nservers, "nservers", 5, "Number of servers")
	flag.BoolVar(&_unreliable, "unreliable", false, "Whether the network is unreliable")
	flag.IntVar(&_dnsDuration, "dnsDuration", 10, "Duration of the dns test")
	flag.BoolVar(&_crash, "crash", false, "Whether to crash servers")

	flag.Parse()

	fmt.Printf("[INFO] : nclerks: %v, nservers: %v, unreliable: %v, dnsDuration: %v, crash: %v\n", _nclerks, _nservers, _unreliable, _dnsDuration, _crash)

	// Metadata & make config
	title := "Start"
	nclients := _nclerks
	unreliable := _unreliable
	maxraftstate := 1000
	dnsDuration := _dnsDuration
	nservers := _nservers
	crash:=_crash
	cfg := my_make_config(nservers, unreliable, maxraftstate)
	defer cfg.Cleanup()

	cfg.Begin(title)

	ClerkAddresses := my_make_dns_address(nclients)
	context, cancelFunc := context.WithCancel(context.Background())

	// Start the clerk routines
	cfg.Begin(title)
	fmt.Printf("[INFO] : Starting all clients... time: %v\n", time.Now())
	ca := make([]chan bool, nclients)
	for cli := 0; cli < nclients; cli++ {
		ca[cli] = make(chan bool)
		go my_run_client(cfg, cli, ca[cli], ClerkAddresses[cli], clerk_routine, context)
	}

	if crash {
		// Make sure that at most half of the servers are crashed
		maxCrash := nservers / 2
		crashedServers := make(map[int]bool) // Record the crashed servers

		var mutex sync.Mutex = sync.Mutex{}

		// Start the crash controller routines
		for i := 0; i < nservers; i++ {
			go func(i int) {
				for {
					select {
					case <-context.Done(): // If the global context is done, exit
						fmt.Printf("Crash Controller %v received done signal, exiting...\n", i)
						return
					default:
						time.Sleep(time.Duration(rand.Intn(1000)+1000) * time.Millisecond)

						mutex.Lock()
						// Check if enough servers have crashed
						if len(crashedServers) >= maxCrash {
							// If enough servers have crashed, do nothing
							mutex.Unlock()
							continue
						}
						// If not, crash this server
						cfg.ShutdownServer(i)
						fmt.Printf("shutdown server %d, time: %v\n", i, time.Now())
						crashedServers[i] = true // Record the crashed server
						mutex.Unlock()

						// Later, random start the server
						time.Sleep(time.Duration(rand.Intn(1000)+1000) * time.Millisecond)
						
						mutex.Lock()
						cfg.StartServer(i)
						cfg.ConnectAll()
						fmt.Printf("start server %d, time: %v\n", i, time.Now())
						// Update the crashedServers map
						delete(crashedServers, i)
						mutex.Unlock()
					}
				}
			}(i)
		}
		cfg.ConnectAll()
	}

	// Use this to set the time before the program exits
	time.Sleep(time.Duration(dnsDuration) * time.Second)

	// Tell clients to quit
	cancelFunc()
	fmt.Printf("[INFO] : Shutting down clients... time: %v\n", time.Now())
	
	// Wait for all clients to finish, and check if they succeeded
	for cli := 0; cli < nclients; cli++ {
		ok := <-ca[cli]
		if ok == false {
			log.Fatal("failure")
		}
	}

	// Show total statistics
	fmt.Printf("===============Statistics===============\n")
	fmt.Printf("Total packets: %v, total put: %v, total query: %v \n", total_received, total_put, total_query)

	cfg.End()
}
