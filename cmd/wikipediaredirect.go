/*
Copyright © 2023 NAME HERE <EMAIL ADDRESS>

*/
package cmd

import (
        "bytes"
        "compress/gzip"
	"database/sql"
        "fmt"
	"log"
        "os"

        "github.com/pingcap/tidb/parser"
        "github.com/pingcap/tidb/parser/ast"
        _ "github.com/pingcap/tidb/parser/test_driver"

	"github.com/spf13/cobra"
	mssql "github.com/microsoft/go-mssqldb"
)


// redirectCmd represents the redirect command
var redirectCmd = &cobra.Command{
	Use:   "redirect",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("wikipedia redirect called")
		readRedirectDataset()
	},
}


var (  // flag
	redirectdataset string
)

type Pair struct {
        From int64
        To string
}
type Pairs struct {
        Values []Pair
}

func (ps *Pairs) Enter(in ast.Node) (ast.Node, bool) {
        if vs, ok := in.(*ast.InsertStmt); ok {
                for _, elems := range vs.Lists {
                        fromid, _ := elems[0].(ast.ValueExpr)
                        totitle, _ := elems[2].(ast.ValueExpr)
                        from := fromid.GetValue().(int64)
                        to := totitle.GetDatumString()
                        p := &Pair{from, to}
                        ps.Values = append(ps.Values, *p)
                }
        }
        return in, false
}
func (ps *Pairs) Leave(in ast.Node) (ast.Node, bool) {
        return in, true
}

func parse(sql string) (*[]ast.StmtNode, error) {
        p := parser.New()

        stmtNodes, _, err := p.Parse(sql, "", "")
        if err != nil {
                return nil, err
        }

        return &stmtNodes, nil
}

func extract(rootNode *ast.StmtNode) *Pairs {
        ps := &Pairs{}
        (*rootNode).Accept(ps)
        return ps
}

func readRedirectDataset() {
	f, err := os.Open(redirectdataset)
        defer f.Close()
        gz, err := gzip.NewReader(f)
        defer gz.Close()
        buf := new(bytes.Buffer)
        buf.ReadFrom(gz)
        astNodes, err := parse(buf.String())
        if err != nil {
                fmt.Printf("parse error: %v\n", err.Error())
                return
        }
	db := openSQL(server, database, user, password)
        for _, astNode := range *astNodes {
		pairs := extract(&astNode)
		if len(pairs.Values) > 0 {
			loadRedirectSQL(db, pairs)
		}
        }
}


func loadRedirectSQL(db *sql.DB, pairs *Pairs) {
	/*
	CREATE SCHEMA wikipedia;
	GO
	CREATE TABLE wikipedia.redirects (
		id bigint,
		totitle nvarchar(max),
		CONSTRAINT [PK_redirect_id] PRIMARY KEY CLUSTERED
		(
			id ASC
		) ON [PRIMARY]);
	*/
	txn, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	stmt, err := txn.Prepare(mssql.CopyIn("wikipedia.redirects", mssql.BulkOptions{}, "id", "totitle"))
	if err != nil {
		log.Fatal(err.Error())
	}
	for _, pair := range (*pairs).Values {
		_, err = stmt.Exec(pair.From, pair.To)
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
	wikipediaCmd.AddCommand(redirectCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// redirectCmd.PersistentFlags().String("foo", "", "A help for foo")
	redirectCmd.PersistentFlags().StringVarP(&redirectdataset, "dataset", "", "", "dataset (xml.bz2)")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// redirectCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
