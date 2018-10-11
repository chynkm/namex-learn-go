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

func (db *datastore) ExampleScrape() {
	// Request the HTML page.
	res, err := http.Get("https://www.imdb.com/title/tt5320514")
	if err != nil {
		log.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		log.Fatalf("status code error: %d %s", res.StatusCode, res.Status)
	}

	// Load the HTML document
	doc, err := goquery.NewDocumentFromReader(res.Body)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("title field: %s\n", doc.Find("title").First().Text())

	doc.Find("meta").Each(func(i int, s *goquery.Selection) {
		if name, _ := s.Attr("name"); strings.EqualFold(name, "description") {
			description, _ := s.Attr("content")
			fmt.Printf("Description field: %s\n", description)
		}

		if name, _ := s.Attr("name"); strings.EqualFold(name, "keywords") {
			keywords, _ := s.Attr("content")
			fmt.Printf("keyword field: %s\n", keywords)

			keywordSlice := strings.Split(keywords, ",")
			fmt.Println(keywordSlice)

			sqlStr := "INSERT INTO keywords(domain_id, keyword) VALUES "
			vals := []interface{}{}

			for _, keyword := range keywordSlice {
				sqlStr += "(?, ?),"
				vals = append(vals, 1, strings.TrimSpace(keyword))
			}

			sqlStr = sqlStr[0 : len(sqlStr)-1]
			stmt, _ := db.Prepare(sqlStr)

			_, err := stmt.Exec(vals...)
			if err != nil {
				fmt.Errorf("error in keyword insert query")
			}
		}

		if name, _ := s.Attr("property"); strings.EqualFold(name, "og:title") {
			title, _ := s.Attr("content")
			fmt.Printf("og title field: %s\n", title)
		}

		if name, _ := s.Attr("property"); strings.EqualFold(name, "og:description") {
			description, _ := s.Attr("content")
			fmt.Printf("og description field: %s\n", description)
		}
	})
}

func main() {
	db, _ := open()
	db.ExampleScrape()
}
