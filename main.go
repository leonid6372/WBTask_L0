package main

import (
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"strconv"

	nats "github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
	"github.com/nats-io/stan.go/pb"
	"main.go/cache"
	"main.go/web"
)

const (
	clusterID, clientID = "test-cluster", "service"
	URL                 = "127.0.0.1"
	lastMsgNumFile      = "last_message_number.txt"
)

// Log message from NATS
func printMsg(msg *[]byte) {
	log.Printf("Received message from NATS: \"%s\"\n", *msg)
}

// Update last received message number
func newMsgNumber(num uint64) {
	if err := os.Truncate(lastMsgNumFile, 0); err != nil {
		log.Printf("Failed to truncate: %v", err)
	}
	message_number := strconv.Itoa(int(num))
	if err := ioutil.WriteFile(lastMsgNumFile, []byte(message_number), 0600); err != nil {
		log.Println(err)
	}
}

// Get last received message number if service was crashed. Otherwise get 0
func GetMsgNumber() uint64 {
	num, err := ioutil.ReadFile(lastMsgNumFile)
	if err != nil {
		log.Println(err)
	}
	last_message_number, err := strconv.ParseUint(string(num), 10, 64)
	if err != nil {
		log.Println(err)
	}
	return last_message_number
}

func main() {
	// Update orders in cache from DB
	cache.OrdersToCache()

	// Start http-server as goroutine
	go web.StartWebServer()

	// Connect to NATS
	nc, err := nats.Connect(URL)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	sc, err := stan.Connect(clusterID, clientID, stan.NatsConn(nc),
		stan.SetConnectionLostHandler(func(_ stan.Conn, reason error) {
			log.Fatalf("Connection lost, reason: %v", reason)
		}))
	if err != nil {
		log.Fatalf("Can't connect: %v.\nMake sure a NATS Streaming Server is running at: %s", err, URL)
	}
	log.Printf("Connected to %s clusterID: [%s] clientID: [%s]\n", URL, clusterID, clientID)

	// Process Subscriber Options
	// If service was crashed start with last received message
	startOpt := stan.StartAt(pb.StartPosition_NewOnly)
	last_message_number := GetMsgNumber()
	if last_message_number != 0 {
		startOpt = stan.StartAtSequence(last_message_number)
	}

	// Log, validate message from NATS and record to DB
	subj := "service-channel"
	mcb := func(m *stan.Msg) {
		printMsg(&m.MsgProto.Data)
		cache.RecordNewOrder(&m.MsgProto.Data)
		newMsgNumber(m.Sequence)
	}

	// Make Subscription
	sub, err := sc.Subscribe(subj, mcb, startOpt)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Listening on [%s]", subj)
	// Wait for a SIGINT (perhaps triggered by user with CTRL-C)
	// Run cleanup when signal is received
	signalChan := make(chan os.Signal, 1)
	cleanupDone := make(chan bool)
	signal.Notify(signalChan, os.Interrupt)
	go func() {
		for range signalChan {
			log.Printf("\nReceived an interrupt, unsubscribing and closing connection...\n\n")
			if err := sub.Unsubscribe(); err != nil {
				log.Fatal(err)
			}
			sc.Close()
			// Finishing is correct, that why set last message number in 0
			newMsgNumber(0)
			cleanupDone <- true
		}
	}()
	<-cleanupDone
}
