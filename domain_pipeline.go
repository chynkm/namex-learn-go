package main

import (
	"database/sql"
	"fmt"
	"github.com/PuerkitoBio/goquery"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"net/http"
	"strings"
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
}

func ExampleScrape(domainName DomainName) (domainInfo DomainInfo) {
	domainInfo.DomainId = domainName.Id

	// Request the HTML page.
	res, err := http.Get("http://" + domainName.Domain)
	if err != nil {
		log.Println("domain name %s fetch failed", domainName.Domain)
		return
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		log.Fatalf("status code error: %d %s", res.StatusCode, res.Status)
	}

	// Load the HTML document
	doc, err := goquery.NewDocumentFromReader(res.Body)
	failOnError(err, "HTML document loading failure")

	// fmt.Printf("title field: %s\n", doc.Find("title").First().Text())
	domainInfo.Title = doc.Find("title").First().Text()

	doc.Find("meta").Each(func(i int, s *goquery.Selection) {
		if name, _ := s.Attr("name"); strings.EqualFold(name, "description") {
			description, _ := s.Attr("content")
			// fmt.Printf("Description field: %s\n", description)
			domainInfo.Description = description
		}

		if name, _ := s.Attr("name"); strings.EqualFold(name, "keywords") {
			keywords, _ := s.Attr("content")
			// fmt.Printf("keyword field: %s\n", keywords)
			domainInfo.Keywords = keywords
		}

		if name, _ := s.Attr("property"); strings.EqualFold(name, "og:title") {
			ogTitle, _ := s.Attr("content")
			// fmt.Printf("og title field: %s\n", ogTitle)
			domainInfo.OgTitle = ogTitle
		}

		if name, _ := s.Attr("property"); strings.EqualFold(name, "og:description") {
			ogDescription, _ := s.Attr("content")
			// fmt.Printf("og description field: %s\n", ogDescription)
			domainInfo.OgDescription = ogDescription
		}
	})
	return
}

func (db *datastore) InsertKeywords(domainInfo DomainInfo) {
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

	rows, err := db.Query(`SELECT id, domain FROM domains_new WHERE expiry_date is NULL`)
	failOnError(err, "Error in SQL select statement")
	defer rows.Close()

	for rows.Next() {
		err = rows.Scan(&d.Id, &d.Domain)
		failOnError(err, "Error in SQL scan statement")

		fmt.Println(d)

		domain <- d
	}
	close(domain)
}

func processor(domain <-chan DomainName, domainInfo chan<- DomainInfo) {
	for d := range domain {
		domainInfo <- ExampleScrape(d)
	}
}

func (db *datastore) consumer(domainInfo <-chan DomainInfo) {
	for d := range domainInfo {
		// fmt.Println(d)
		db.InsertKeywords(d)
	}
}

func main() {
	// d := DomainName{1, "imdb.com/title/tt0493405"}
	// ExampleScrape(d)
	db, _ := open()
	loop := make(chan bool)

	domain := make(chan DomainName)
	domainInfo := make(chan DomainInfo)
	for i := 0; i < 20; i++ {
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
