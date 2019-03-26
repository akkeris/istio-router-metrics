package main

import (
	"fmt"
	cluster "github.com/bsm/sarama-cluster"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"
)

var conn net.Conn
var err error
var processed int
var debugnosend bool
var debugoutput bool
var sendfwdtlsversion bool

func main() {
	processed = 0
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

	t := time.Now()
	consumerindex := t.Format("2006-01-02T15:04:05.999999-07:00")
	consumergroupnamebase := os.Getenv("CONSUMER_GROUP_NAME")
	consumergroupname := consumergroupnamebase + "-" + consumerindex
	fmt.Println(consumergroupname)
	conn, err = net.Dial("udp", os.Getenv("INFLUX_LINE_IP"))
	if err != nil {
		fmt.Println("dial error:", err)
		os.Exit(1)
	}

	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true
	brokers := strings.Split(os.Getenv("KAFKA_BROKERS"), ",")
	topics := []string{"alamoweblogs"}
	consumer, err := cluster.NewConsumer(brokers, consumergroupname, topics, config)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer consumer.Close()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	signal.Notify(signals, os.Kill)

	consumed := 0

	for {
		select {
		case msg, more := <-consumer.Messages():
			if more {
				if debugoutput {
					fmt.Fprintf(os.Stdout, "%s/%d/%d\t%s\t%s\n", msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
				}
				sock(string(msg.Value[:]))
				consumer.MarkOffset(msg, "") // mark message as processed
				consumed++
			}
		case err, more := <-consumer.Errors():
			if more {
				log.Printf("Error: %s\n", err.Error())
			}
		case ntf, more := <-consumer.Notifications():
			if more {
				log.Printf("Rebalanced: %+v\n", ntf)
			}
		case <-signals:
			return
		}
	}

	log.Printf("Consumed: %d\n", consumed)
	log.Printf("Processed: %d\n", processed)
	conn.Close()

}

func sock(logline string) {
	words := strings.Fields(logline)
	var fieldmap map[string]string
	fieldmap = make(map[string]string)
	for _, element := range words {
		if strings.Contains(element, "=") {
			fieldmap[strings.Split(element, "=")[0]] = strings.Split(element, "=")[1]
		}
	}
	host := fieldmap["hostname"]
	var fields string
	var tags string
        if strings.Contains(logline,"code=H27") {
           fmt.Println(words)
           fmt.Println(fieldmap)
           status := fieldmap["status"]
           client_closed_time := fieldmap["client_closed_time"]
           site_domain := fieldmap["site_domain"]
           fwd := "\"" + strings.Split(fieldmap["fwd"], "%")[0] + "\""
           tlsversion := "\"" + fieldmap["tlsversion"] + "\""
           if sendfwdtlsversion {
                  fields = "fwd=" + fwd + ",tlsversion=" + tlsversion + ","
           }
           tags = "fqdn=" + host
           metricname := "router.client.closed.count"
           value := "1"
           send(metricname, tags, fields+"value="+value)
           metricname = "router.client.closed.ms"
           value = client_closed_time
           send(metricname, tags, fields+"value="+value)
           metricname = "router.status." + status
           value = "1"
           send(metricname, tags, fields+"value="+value)
           metricname = "router.requests.count"
           value = "1"
           send(metricname, tags, fields+"value="+value)
           if site_domain != "" {
                        host = site_domain
                        tags = "fqdn=" + host
                        metricname = "router.client.closed.count"
                        value = "1"
                        send(metricname, tags, fields+"value="+value)
                        metricname = "router.client.closed.ms"
                        value = client_closed_time
                        send(metricname, tags, fields+"value="+value)
                        metricname = "router.status." + status
                        value = "1"
                        send(metricname, tags, fields+"value="+value)
                        metricname = "router.requests.count"
                        value = "1"
                        send(metricname, tags, fields+"value="+value)
           }
        }

	if !(strings.HasPrefix(host, "alamotest")) && len(words) > 9 && !strings.Contains(strings.Join(words, " "), "4813") && !strings.Contains(logline,"code=H27"){
		status := fieldmap["status"]
		service := fieldmap["service"]
		connect := fieldmap["connect"]
		total := fieldmap["total"]
		site_domain := fieldmap["site_domain"]
		fwd := "\"" + strings.Split(fieldmap["fwd"], "%")[0] + "\""
		tlsversion := "\"" + fieldmap["tlsversion"] + "\""
		tags = "fqdn=" + host
		if sendfwdtlsversion {
			fields = "fwd=" + fwd + ",tlsversion=" + tlsversion + ","
		}
		metricname := "router.service.ms"
		value := service
		send(metricname, tags, fields+"value="+value)

		metricname = "router.total.ms"
		value = total
		send(metricname, tags, fields+"value="+value)

		metricname = "router.connect.ms"
		value = connect
		send(metricname, tags, fields+"value="+value)

		metricname = "router.status." + status
		value = "1"
		send(metricname, tags, fields+"value="+value)

		metricname = "router.requests.count"
		value = "1"
		send(metricname, tags, fields+"value="+value)

		if site_domain != "" {
			host = site_domain
			tags = "fqdn=" + host
			metricname := "router.service.ms"
			value := service
			send(metricname, tags, fields+"value="+value)
			metricname = "router.total.ms"
			value = total
			send(metricname, tags, fields+"value="+value)
			metricname = "router.connect.ms"
			value = connect
			send(metricname, tags, fields+"value="+value)
			metricname = "router.status." + status
			value = "1"
			send(metricname, tags, fields+"value="+value)
			metricname = "router.requests.count"
			value = "1"
			send(metricname, tags, fields+"value="+value)
		}

	}
}

func send(measurement string, tags string, fields string) {
	if !debugnosend {
		fmt.Fprintf(conn, measurement+","+tags+" "+fields+"\n")
	}
	if debugoutput {
		fmt.Println(measurement + "," + tags + " " + fields)
	}
}
