package main

import (
	"fmt"
	"sync/atomic"
	"time"
	"math/rand"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)


var counter uint64
var start = time.Now()
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

func main() {
	exit := make(chan bool, totalPublishes)

	opts := mqtt.NewClientOptions().AddBroker("tcp://localhost:1883")
	opts.SetClientID("mqtt-benchmark")

	msgHandler := func(client mqtt.Client, msg mqtt.Message) {
		c := atomic.AddUint64(&counter, 1)

		if c >= totalPublishes {
			exit <- true
		}
	}


	//create and start a client using the above ClientOptions
	c := mqtt.NewClient(opts)

	if token := c.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	if token := c.Subscribe("hello/mqtt/rumqtt", 1, msgHandler); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	go func() {
		text := data(payloadSize)
		for i := 0; i < totalPublishes; i++ {
			token := c.Publish("hello/mqtt/rumqtt", 1, false, text)
			token.Wait()
		}

		fmt.Println("Throughput = ", (totalPublishes * payloadSize)/1024.0/1024.0/time.Since(start).Seconds(), "MB/s")
	}()

L:
	for {
		select {
		case e := <-exit:
			if e {
				break L
			}
		case <-time.After(5 * time.Second):
			fmt.Println("incoming pub count = ", atomic.LoadUint64(&counter), ". time taken for incoming pubs = ",  time.Since(start))
		}
	}

	fmt.Println("incoming pub count = ", atomic.LoadUint64(&counter), ". time taken for incoming pubs = ",  time.Since(start))
	time.Sleep(1 * time.Second)
}
