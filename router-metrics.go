package main

import (
    "encoding/json"
    "fmt"
    cluster "github.com/bsm/sarama-cluster"
    "log"
    "net"
    "os"
    "os/signal"
    "strconv"
    "strings"
    "time"
    //"github.com/Shopify/sarama.v1"
    "gopkg.in/Shopify/sarama.v1"
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
    conn, err = net.Dial("tcp", os.Getenv("INFLUX"))
    if err != nil {
        fmt.Println("dial error:", err)
        os.Exit(1)
    }

    config := cluster.NewConfig()
    config.Consumer.Return.Errors = true
    config.Consumer.Offsets.Initial = sarama.OffsetNewest

    config.Group.Return.Notifications = true
    brokers := strings.Split(os.Getenv("KAFKA_BROKERS"), ",")
    topics := []string{"istio-access-logs"}
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
                sock(msg.Value, msg.Timestamp)
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

func sock(logline []byte, timestamp time.Time) {
    var l IstioAccessLog
    err := json.Unmarshal(logline, &l)
    if err != nil {
        fmt.Println(err)
    }
    var tags string
    tags = "fqdn=" + l.Host
    send("router.service.ms", tags, fixTimeValue(l.Service), timestamp)
    send("router.total.ms", tags, fixTimeValue(l.Total), timestamp)
    send("router.status."+strconv.Itoa(l.Status), tags, "1", timestamp)
    send("router.requests.count", tags, "1", timestamp)
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

