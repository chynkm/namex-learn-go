package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"io/ioutil"
	"log"
	"net"
	"regexp"
	"strings"
	"time"
)

type datastore struct {
	*sql.DB
}

func open() (*datastore, error) {
	db, err := sql.Open("mysql", "root:@tcp(192.168.1.2)/namex?parseTime=true")

	if err != nil {
		log.Panic(err)
		return nil, err
	}

	if err = db.Ping(); err != nil {
		log.Panic(err)
		return nil, err
	}
	return &datastore{db}, nil
}

func main() {
	db, _ := open()
	_ = db.updateExpiryDates()
	// whoisInfo, err := getWhois("google.co.in")

	// if err != nil {
	// 	fmt.Errorf("error in getWhois")
	// 	return
	// }

	// expiryDate := getExpiry(whoisInfo)
	// fmt.Println(expiryDate)
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

func (db *datastore) updateExpiryDates() (err error) {
	var id int64
	var domain string
	var stmt *(sql.Stmt)

	stmt, err = db.Prepare("UPDATE domains SET expiry_date = ? WHERE id = ?")
	if err != nil {
		fmt.Println("There was an error in update query")
		return
	}

	rows, err := db.Query(`SELECT id, domain FROM domains WHERE expiry_date is NULL`)
	// rows, err := db.Query(`SELECT id, domain FROM domains WHERE expiry_date is NULL or expiry_date < NOW()`)

	if err != nil {
		fmt.Println("There was an error in fetching hosts for probing")
		return err
	}

	defer rows.Close()

	for rows.Next() {
		err = rows.Scan(&id, &domain)
		if err != nil {
			return err
		}

		whoisInfoChannel := make(chan string)
		go func() {
			whoisInfo, _ := getWhois(domain)
			whoisInfoChannel <- whoisInfo
		}()

		if err != nil {
			fmt.Errorf("error in getWhois")
			// return err
		}

		expiryDate := getExpiry(<-whoisInfoChannel)
		fmt.Println(domain, expiryDate)

		_, err = stmt.Exec(expiryDate, id)
		if err != nil {
			fmt.Errorf("error in update query")
			return err
		}
	}

	return
}
