package main

import (
	"os"
	"fmt"
	"sync/atomic"
	"time"
	"math/rand"
	progressbar "github.com/schollz/progressbar/v2"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)


var counter uint64
var end = time.Now()

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
	total int
	client mqtt.Client
	stats chan uint64
	progress chan uint64
}

func NewConnection(id string, total int, stats, progress chan uint64) *Connection {
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
		progress: progress,
	}
}

func (c *Connection) Start() {
	var counter uint64
	var start = time.Now()
	exit := make(chan bool, 10)

	msgHandler := func(client mqtt.Client, msg mqtt.Message) {
		count := atomic.AddUint64(&counter, 1)
		if count == uint64(c.total) {
			exit <- true
		}
	}

	if token := c.client.Subscribe("hello/mqtt/rumqtt", 1, msgHandler); token.Wait() && token.Error() != nil {
		fmt.Println("Error = ", token.Error())
		os.Exit(1)
	}

	go func() {
		text := data(payloadSize)
		for i := 0; i < c.total ; i++ {
			token := c.client.Publish("hello/mqtt/rumqtt", 1, false, text)
			token.Wait()
		}
	}()
	
	for {
		select {
		case <-time.After(100 * time.Millisecond):
			c.progress <- counter
		case <-exit:
			c.progress <- counter
			totalSize := float64(c.total * payloadSize)
			throughput := totalSize/1024.0/1024.0/time.Since(start).Seconds()
			c.stats <- uint64(throughput)
			return	
		}
	}
}

func main() {
	exit := make(chan uint64, 10)
	progress := make(chan uint64, 100)
	totalConnections := 1
	msgsPerConnection := 100000
	totalDone := 0
	throughputs := make([]uint64, 0)
	percetage := totalConnections * msgsPerConnection
	progressbar := progressbar.NewOptions(percetage, progressbar.OptionSetTheme(progressbar.Theme{Saucer: "|", SaucerPadding: "-"}))
	// var start = time.Now()

	for i := 0; i < totalConnections; i++ {
		id := fmt.Sprintf("bench-%v", i)
		connection := NewConnection(id, msgsPerConnection, exit, progress)
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
		case p := <- progress:
			progressbar.Set(int(p))
		}
	}


	count := len(throughputs)
	var total uint64 = 0
	for i := 0; i < count; i++ {
		total += throughputs[i]
	}

	fmt.Println("\n\n\nAverage throughput = ", float64(total)/float64(count), "MB/s")
}
