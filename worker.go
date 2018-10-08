package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/streadway/amqp"
	"io/ioutil"
	"log"
	"net"
	"regexp"
	"strings"
	"time"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"task_queue", // name
		true,         // durable
		false,        // delete when unused
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	failOnError(err, "Failed to set QoS")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	db, _ := open()
	var stmt *(sql.Stmt)
	stmt, err = db.Prepare("UPDATE domains SET expiry_date = ? WHERE id = ?")
	if err != nil {
		failOnError(err, "There was an error in update query")
	}

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			var domainName DomainName
			fmt.Println("achu")

			err = json.Unmarshal(d.Body, &domainName)
			whoisInfo, _ := getWhois(domainName.Domain)
			expiryDate := getExpiry(whoisInfo)
			fmt.Println(domainName.Domain, expiryDate)

			_, err = stmt.Exec(expiryDate, domainName.Id)
			if err != nil {
				failOnError(err, "error in update query")
			}
			log.Printf("Done")
			d.Ack(false)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}

func getWhois(domain string) (result string, err error) {
	var (
		parts      []string
		zone       string
		buffer     []byte
		connection net.Conn
	)

	parts = strings.Split(domain, ".")
	if len(parts) < 2 {
		err = fmt.Errorf("Domain(%s) name is wrong!", domain)
		return
	}

	zone = parts[len(parts)-1]
	server, ok := servers[zone]
	if !ok {
		err = fmt.Errorf("No such server for zone %s. Domain %s.", zone, domain)
		return
	}

	connection, err = net.DialTimeout("tcp", net.JoinHostPort(server, "43"), time.Second*5)
	if err != nil {
		return
	}
	defer connection.Close()

	connection.Write([]byte(domain + "\r\n"))

	buffer, err = ioutil.ReadAll(connection)

	if err != nil {
		return
	}

	result = string(buffer[:])
	return
}

func getExpiry(whoisInfo string) (expiryDate string) {
	regex := regexp.MustCompile(`Registry Expiry Date:.*`)
	match := regex.FindStringSubmatch(whoisInfo)
	if match != nil {
		expiryDate = strings.Replace(match[0], "Registry Expiry Date: ", "", 1)
		expiryDate = strings.Replace(expiryDate, "T", " ", 1)
		expiryDate = strings.Replace(expiryDate, "Z", "", 1)
	}
	return
}
