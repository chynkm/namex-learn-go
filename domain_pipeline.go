package main

import (
	"database/sql"
	"fmt"
	"github.com/PuerkitoBio/goquery"
	_ "github.com/go-sql-driver/mysql"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"regexp"
	"strings"
	"time"
)

type DomainName struct {
	Id     int64
	Domain string
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

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

type DomainInfo struct {
	DomainId      int64
	Title         string
	OgTitle       string
	Description   string
	OgDescription string
	Keywords      string
	ExpiryDate    string
}

func exampleScrape(domainName DomainName) (domainInfo DomainInfo) {
	domainInfo.DomainId = domainName.Id

	// Request the HTML page.
	res, err := http.Get("http://" + domainName.Domain)
	if err != nil {
		res, err = http.Get("https://" + domainName.Domain)
		if err != nil {
			log.Printf("domain name %s fetch failed", domainName.Domain)
			return
		}
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		log.Fatalf("status code error: %d %s", res.StatusCode, res.Status)
	}

	// Load the HTML document
	doc, err := goquery.NewDocumentFromReader(res.Body)
	failOnError(err, "HTML document loading failure")

	domainInfo.Title = doc.Find("title").First().Text()

	doc.Find("meta").Each(func(i int, s *goquery.Selection) {
		if name, _ := s.Attr("name"); strings.EqualFold(name, "description") {
			description, _ := s.Attr("content")
			domainInfo.Description = description
		}

		if name, _ := s.Attr("name"); strings.EqualFold(name, "keywords") {
			keywords, _ := s.Attr("content")
			domainInfo.Keywords = keywords
		}

		if name, _ := s.Attr("property"); strings.EqualFold(name, "og:title") {
			ogTitle, _ := s.Attr("content")
			domainInfo.OgTitle = ogTitle
		}

		if name, _ := s.Attr("property"); strings.EqualFold(name, "og:description") {
			ogDescription, _ := s.Attr("content")
			domainInfo.OgDescription = ogDescription
		}
	})

	whoisInfo, err := getWhois(domainName.Domain)
	if err != nil {
		log.Println("WHOIS fetch fetch failed", err, domainName.Domain)
	}
	domainInfo.ExpiryDate = getExpiry(whoisInfo)

	return
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

func (db *datastore) insertKeywords(domainInfo DomainInfo) {
	keywordSlice := strings.Split(domainInfo.Keywords, ",")

	sqlStr := "INSERT INTO keywords(domain_id, keyword) VALUES "
	vals := []interface{}{}

	for _, keyword := range keywordSlice {
		trimmedKeyword := strings.TrimSpace(keyword)
		if len(trimmedKeyword) > 0 {
			sqlStr += "(?, ?),"
			vals = append(vals, domainInfo.DomainId, trimmedKeyword)
		}
	}

	if len(vals) > 0 {
		sqlStr = sqlStr[0 : len(sqlStr)-1]
		stmt, _ := db.Prepare(sqlStr)

		_, err := stmt.Exec(vals...)
		failOnError(err, "Error in keyword insert query")
	}
}

func (db *datastore) producer(domain chan<- DomainName) {
	d := DomainName{}

	rows, err := db.Query(`SELECT id, domain FROM domains WHERE expiry_date is NULL`)
	failOnError(err, "Error in SQL select statement")
	defer rows.Close()

	for rows.Next() {
		err = rows.Scan(&d.Id, &d.Domain)
		failOnError(err, "Error in SQL scan statement")

		domain <- d
	}
	close(domain)
}

func processor(domain <-chan DomainName, domainInfo chan<- DomainInfo) {
	for d := range domain {
		domainInfo <- exampleScrape(d)
	}
}

func (db *datastore) updateDomain(d DomainInfo) {
	var stmt *(sql.Stmt)
	stmt, err := db.Prepare("UPDATE domains SET title = ?, description = ?, og_title = ?, og_description = ?, expiry_date = ? WHERE id = ?")
	failOnError(err, "There was an error in update prepare query")

	_, err = stmt.Exec(d.Title, d.Description, d.OgTitle, d.OgDescription, d.ExpiryDate, d.DomainId)
	failOnError(err, "Error in update query execution")
}

func (db *datastore) consumer(domainInfo <-chan DomainInfo) {
	for d := range domainInfo {
		fmt.Println(d.Domain)
		db.insertKeywords(d)
		db.updateDomain(d)
	}
}

func main() {
	db, _ := open()
	loop := make(chan bool)

	domain := make(chan DomainName)
	domainInfo := make(chan DomainInfo)
	for i := 0; i < 100; i++ {
		go func() {
			go processor(domain, domainInfo)
		}()
		go func() {
			go db.consumer(domainInfo)
		}()
	}

	go db.producer(domain)
	<-loop
}
