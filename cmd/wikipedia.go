/*
Copyright © 2023 NAME HERE <EMAIL ADDRESS>

*/
package cmd

import (
	"bufio"
	"compress/bzip2"
	"database/sql"
	"encoding/xml"
	"fmt"
	"io"
	"os"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	mssql "github.com/microsoft/go-mssqldb"
)

// wikipediaCmd represents the wikipedia command
var wikipediaCmd = &cobra.Command{
	Use:   "wikipedia",
	Short: "load wikipedia page data",
	Long: `.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("wikipedia called")
		readDataset()
	},
}

var (  // flags
       dataset string
       index string
)

type Page struct {
	Title string
	Id uint64
	Timestamp time.Time
	Model string
	Format string
	Text string
}

type Pages []*Page

func readDataset() {
	var fi *os.File
	var fd *os.File
	var err error
	// open index
	if fi, err = os.Open(index); err != nil {
		log.Fatalln(err)
	}
	defer fi.Close()
	// read index
	r := bzip2.NewReader(fi)
	scanner := bufio.NewScanner(r)
	var indexp = make(map[int]int)
	var prev int
	for scanner.Scan() {
		ent := scanner.Text()
		elms := strings.SplitN(ent, ":", 3)
		start, _ := strconv.Atoi(elms[0])
		if prev != start {
			indexp[prev] = start
		}
		prev = start
        }
	// open dataset
	if fd, err = os.Open(dataset); err != nil {
		log.Fatalln(err)
	}
	defer fd.Close()
	// open database
	db := openSQL(server, database, user, password)
	// read dataset
	for start, next := range indexp {
		p := io.NewSectionReader(fd, int64(start), int64(next - start))
		part := bzip2.NewReader(p)
		/*
		buf := new(strings.Builder)
		if _, err := io.Copy(buf, part); err != nil {
			log.Fatal(err)
		}
		fmt.Println(buf.String())
		*/
		xdec := xml.NewDecoder(part)
		var celem string
		var cpage *Page
		pages := make(Pages, 0)
		for {
			token, err := xdec.Token();
			if err != nil {
				if err == io.EOF {
					break
				}
				log.Fatal(err)
			}
			if start, ok := token.(xml.StartElement); ok {
				celem = start.Name.Local
				if celem  == "page" {
					cpage = new(Page)
				}
			} else if end, ok := token.(xml.EndElement); ok {
				if end.Name.Local  == "page" {
					pages = append(pages, cpage)
				}
				celem = ""
			}
			if chars, ok := token.(xml.CharData); ok {
				if celem == "title" {
					cpage.Title = string(chars)
				}
				if celem == "id" {
					if cpage.Id == 0 {
						pid, _ := strconv.Atoi(string(chars))
						cpage.Id = uint64(pid)
					}
				}
				if celem == "timestamp" {
					cpage.Timestamp, _ = time.Parse("2006-01-02T15:04:05Z", string(chars))
				}
				if celem == "model" {
					cpage.Model = string(chars)
				}
				if celem == "format" {
					cpage.Format = string(chars)
				}
				if celem == "text" {
					cpage.Text = string(chars)
				}
			}
		}
		// load
		loadSQL(db, &pages)
	}
}

func loadSQL(db *sql.DB, pages *Pages) {
	/*
	CREATE SCHEMA wikipedia;
	GO
	CREATE TABLE wikipedia.pages (
		title nvarchar(max),
		id bigint,
		timestamp datetime,
		model nvarchar(20),
		format nvarchar(20),
		text nvarchar(max),
		CONSTRAINT [PK_page_id] PRIMARY KEY CLUSTERED
		(
			id ASC
		) ON [PRIMARY]);
	*/
	txn, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	stmt, err := txn.Prepare(mssql.CopyIn("wikipedia.pages", mssql.BulkOptions{}, "title", "id", "timestamp", "model", "format", "text"))
	if err != nil {
		log.Fatal(err.Error())
	}
	for _, page := range *pages {
		_, err = stmt.Exec((*page).Title, (*page).Id, (*page).Timestamp, (*page).Model, (*page).Format, (*page).Text)
		if err != nil {
			log.Fatal(err.Error())
		}
	}
	_, err = stmt.Exec()
	if err != nil {
		log.Fatal(err)
	}
	err = stmt.Close()
	if err != nil {
		log.Fatal(err)
	}
	err = txn.Commit()
	if err != nil {
		log.Fatal(err)
	}
}


func init() {
	rootCmd.AddCommand(wikipediaCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// wikipediaCmd.PersistentFlags().String("foo", "", "A help for foo")
	wikipediaCmd.PersistentFlags().StringVarP(&dataset, "dataset", "", "", "dataset (xml.bz2)")
	wikipediaCmd.PersistentFlags().StringVarP(&index, "index", "", "", "index (txt.bz2)")
	wikipediaCmd.MarkFlagsRequiredTogether("dataset", "index")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// wikipediaCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
