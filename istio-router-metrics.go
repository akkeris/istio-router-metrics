package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type IstioAccessLog struct {
	Source    string `json:"source"`
	Method    string `json:"method"`
	App       string `json:"app"`
	Bytes     int    `json:"bytes"`
	Dyno      string `json:"dyno"`
	Severity  string `json:"severity"`
	From      string `json:"from"`
	Space     string `json:"space"`
	Service   string `json:"service"`
	Total     string `json:"total"`
	RequestID string `json:"request_id"`
	Path      string `json:"path"`
	Fwd       string `json:"fwd"`
	Host      string `json:"host"`
	Status    int    `json:"status"`
}

var conn net.Conn
var err error
var debugnosend bool
var debugoutput bool
var sendfwdtlsversion bool
var Blacklist []string
func main() {
	envdebugnosend := os.Getenv("DEBUG_NO_SEND")
	if envdebugnosend == "" {
		envdebugnosend = "false"
	}
	debugnosend, err = strconv.ParseBool(envdebugnosend)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	envdebugoutput := os.Getenv("DEBUG_OUTPUT")
	if envdebugoutput == "" {
		envdebugoutput = "false"
	}
	debugoutput, err = strconv.ParseBool(envdebugoutput)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	envsendfwdtlsversion := os.Getenv("SEND_FWD_TLSVERSION")
	if envsendfwdtlsversion == "" {
		envsendfwdtlsversion = "false"
	}
	sendfwdtlsversion, err = strconv.ParseBool(envsendfwdtlsversion)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

        blacklist_string := os.Getenv("SPACE_BLACKLIST")
        Blacklist = strings.Split(blacklist_string,",")
        fmt.Printf("Space Blacklist: %+v\n", Blacklist)

	t := time.Now()
	consumerindex := t.Format("2006-01-02T15:04:05.999999-07:00")
	consumergroupnamebase := os.Getenv("CONSUMER_GROUP_NAME")
	consumergroupname := consumergroupnamebase + "-" + consumerindex
	fmt.Println(consumergroupname)
	conn, err = net.Dial("tcp", os.Getenv("INFLUX"))
	if err != nil {
		fmt.Println("dial error:", err)
		os.Exit(1)
	}

	version, err := sarama.ParseKafkaVersion("2.0.0")
	if err != nil {
		log.Panicf("Error parsing Kafka version: %v", err)
	}

	config := sarama.NewConfig()
	config.Version = version
	config.Consumer.Return.Errors = true
	brokers := strings.Split(os.Getenv("KAFKA_BROKERS"), ",")
	fmt.Println(brokers)
	topics := []string{"istio-access-logs"}
	group := os.Getenv("CONSUMER_GROUP_NAME")
	consumer := Consumer{
		ready: make(chan bool),
	}
	ctx, cancel := context.WithCancel(context.Background())
	client, err := sarama.NewConsumerGroup(brokers, group, config)
	if err != nil {
		log.Panicf("Error creating consumer group client: %v", err)
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			if err := client.Consume(ctx, topics, &consumer); err != nil {
				log.Panicf("Error from consumer: %v", err)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()

	<-consumer.ready // Await till the consumer has been set up
	log.Println("Sarama consumer up and running!...")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-ctx.Done():
		log.Println("terminating: context cancelled")
	case <-sigterm:
		log.Println("terminating: via signal")
	}
	cancel()
	wg.Wait()
	if err = client.Close(); err != nil {
		log.Panicf("Error closing client: %v", err)
	}

}

type Consumer struct {
	ready chan bool
}

func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	for message := range claim.Messages() {
		if debugoutput {
			log.Printf("Message claimed: value = %s, timestamp = %v, topic = %s", string(message.Value), message.Timestamp, message.Topic)
		}
		sock(message.Value, message.Timestamp)
		session.MarkMessage(message, "")
	}

	return nil
}

func contains(s []string, e string) bool {
    for _, a := range s {
        if a == e {
            return true
        }
    }
    return false
}

func sock(logline []byte, timestamp time.Time) {
	var l IstioAccessLog
	err := json.Unmarshal(logline, &l)
	if err != nil {
		fmt.Println(err)
	}
        if ! (contains(Blacklist, l.Space)) {
	   var tags string
	   tags = "fqdn=" + l.Host
	   send("router.service.ms", tags, fixTimeValue(l.Service), timestamp)
	   send("router.total.ms", tags, fixTimeValue(l.Total), timestamp)
	   send("router.status."+strconv.Itoa(l.Status), tags, "1", timestamp)
	   send("router.requests.count", tags, "1", timestamp)
        }else{
            if debugoutput {
                fmt.Printf("BLACKLISTED/NOT SENDING %+v\n", string(logline))
            }
        }

}

func send(measurement string, tags string, value string, timestamp time.Time) {
	timestamp2 := strconv.FormatInt(time.Now().UnixNano()/int64(time.Millisecond), 10)
	put := "put " + measurement + " " + timestamp2 + " " + value + " " + tags + "\n"
	fmt.Fprintf(conn, put)
}

func fixTimeValue(value string) (nv string) {
	var toreturn string
	if strings.Contains(value, "ms") {
		toreturn = strings.Replace(value, "ms", "", -1)
		return toreturn
	}
	if strings.Contains(value, "µ") {
		nounits := strings.Replace(value, "µ", "", -1)
		converted, _ := strconv.ParseFloat(nounits, 64)
		multiplied := converted * 0.001
		toreturn = fmt.Sprintf("%f", multiplied)
		return toreturn
	}
	return toreturn
}
