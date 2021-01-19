package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"time"

	arg "github.com/alexflint/go-arg"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

var opts struct {
	Messages    int `arg:"-m" help:"Number of messages per connection"`
	PayloadSize int `arg:"-s" help:"Size of each message"`
}

func init() {
	opts.Messages = 100000
	opts.PayloadSize = 100

	arg.MustParse(&opts)
}

func data(n int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}

	return string(b)
}

type Connection struct {
	id     string
	total  int
	client mqtt.Client
}

func NewConnection(id string, total int) *Connection {
	opts := mqtt.NewClientOptions().AddBroker("tcp://localhost:1883")
	opts.SetClientID(id)
	opts.SetProtocolVersion(4)
	opts.SetCleanSession(true)
	opts.SetKeepAlive(10 * time.Second)

	c := mqtt.NewClient(opts)
	if token := c.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	return &Connection{
		id:     id,
		client: c,
		total:  total,
	}
}

func (c *Connection) Start() {
	var start = time.Now()

	text := data(opts.PayloadSize)
	for i := 0; i < c.total; i++ {
		token := c.client.Publish("hello/world", 1, false, text)
		token.Wait()
	}

	timeTaken := time.Since(start).Milliseconds()
	throughputMillis := int64(c.total) / timeTaken
	print := Print {
	    Id: c.id,
	    Messages: c.total,
	    PayloadSize: opts.PayloadSize,
	    Throughput: throughputMillis * 1000,
	}

	p, err := json.MarshalIndent(&print, "", "    ")
	if err != nil {
		log.Fatalln(err)
	}

	fmt.Println(string(p))
}

type Print struct {
    Id string `json:"id"`
    Messages int `json:"messages"`
    PayloadSize int `json:"payload_size"`
    Throughput int64 `json:"throughput"`
}

func main() {
	connection := NewConnection("paho-go", opts.Messages)
	connection.Start()
}
