package main

import (
	"fmt"
	"log"
	// "os"
	// "strings"
	"encoding/json"
	"github.com/streadway/amqp"
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

	// body := bodyFrom(os.Args)
	db, _ := open()
	var id int64
	var domain string

	rows, err := db.Query(`SELECT id, domain FROM domains WHERE expiry_date is NULL`)
	// rows, err := db.Query(`SELECT id, domain FROM domains WHERE expiry_date is NULL or expiry_date < NOW()`)

	if err != nil {
		failOnError(err, "Error in SQL select statement")
	}

	defer rows.Close()

	for rows.Next() {
		err = rows.Scan(&id, &domain)
		if err != nil {
			failOnError(err, "Error in SQL scan statement")
		}

		domainName := DomainName{Id: id, Domain: domain}
		fmt.Println(domainName)

		body, _ := json.Marshal(domainName)

		err = ch.Publish(
			"",     // exchange
			q.Name, // routing key
			false,  // mandatory
			false,
			amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				ContentType:  "text/plain",
				Body:         []byte(body),
				// Body: domianName,
			})
		failOnError(err, "Failed to publish a message")
	}

	// log.Printf(" [x] Sent %s", body)
}
