package main

import (
	"os"
	"fmt"
	"sync/atomic"
	"time"
	"math/rand"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)


var counter uint64
var end = time.Now()

const totalPublishes = 100000
const payloadSize= 1024
const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"


func data(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}

	return string(b)
}

type Connection struct {
	id string
	total uint64
	client mqtt.Client
	stats chan uint64
}

func NewConnection(id string, total uint64, stats chan uint64) *Connection {
	opts := mqtt.NewClientOptions().AddBroker("tcp://localhost:1883")
	opts.SetClientID(id)
	opts.SetProtocolVersion(4)
	opts.SetCleanSession(true)
	opts.SetKeepAlive(10 * time.Second)

	c := mqtt.NewClient(opts)
	if token := c.Connect(); token.Wait() && token.Error() != nil {
		fmt.Println("Error = ", token.Error())
		os.Exit(1)
	}

	return &Connection {
		id: id,
		client: c,
		total: total,
		stats: stats,
	}
}

func (c *Connection) Start() {
	var counter uint64
	var start = time.Now()

	msgHandler := func(client mqtt.Client, msg mqtt.Message) {
		count := atomic.AddUint64(&counter, 1)

		if count == c.total {
			throughput := (totalPublishes * payloadSize)/1024.0/1024.0/time.Since(start).Seconds()
			fmt.Println("Id = ", c.id, "Throughput = ", throughput,  "MB/s")
			c.stats <- uint64(throughput)
		}
	}

	if token := c.client.Subscribe("hello/mqtt/rumqtt", 1, msgHandler); token.Wait() && token.Error() != nil {
		fmt.Println("Error = ", token.Error())
		os.Exit(1)
	}

	go func() {
		text := data(payloadSize)
		for i := 0; i < totalPublishes; i++ {
			token := c.client.Publish("hello/mqtt/rumqtt", 1, false, text)
			token.Wait()
		}
	}()
	
	for {
		<-time.After(100 * time.Second)
		fmt.Println("incoming pub count = ", atomic.LoadUint64(&counter), ". time taken for incoming pubs = ",  time.Since(start))
	}
}

func main() {
	exit := make(chan uint64, 1)
	totalConnections := 10
	totalDone := 0
	throughputs := make([]uint64, 0)
	// var start = time.Now()

	for i := 0; i < totalConnections; i++ {
		time.Sleep(10 * time.Millisecond)
		id := fmt.Sprintf("bench-%v", i)
		connection := NewConnection(id, 10000, exit)
		go connection.Start()
	}

	L:
	for {
		select {
		case throughput := <-exit:
			totalDone += 1
			throughputs = append(throughputs, throughput)
			if totalDone >= totalConnections {
				break L
			}
		case <-time.After(5 * time.Second):
			// fmt.Println("incoming pub count = ", atomic.LoadUint64(&counter), ". time taken for incoming pubs = ",  time.Since(start))
		}
	}


	count := len(throughputs)
	var total uint64 = 0
	for i := 0; i < count; i++ {
		total += throughputs[i]
	}

	time.Sleep(5 * time.Second)
	fmt.Println("Average throughput = ", float64(total)/float64(count), "MB/s")
}
