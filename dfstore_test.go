package dfstore_test

import (
	"context"
	"log"

	"testing"

	"dfstore"
	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
)

var dataRows [][]string

func init() {
	dataRows = [][]string{
		{"title", "artist", "price"},
		{"Blue Train", "John Coltrane", "56.99"},
		{"Giant Steps", "John Coltrane", "63.99"},
		{"Jeru", "Gerry Mulligan", "17.99"},
		{"Sarah Vaughan", "Sarah Vaughan", "34.98"},
	}
}

func TestDefault1(t *testing.T) {
	example1(t, "default")
}

func TestMemory1(t *testing.T) {
	example1(t, "memory")
}

func TestDocument1(t *testing.T) {
	example1(t, "document")
}

func example1(t *testing.T, dbtype string) { 
	dfs, err := dfstore.New(context.TODO(), dbtype)
	if err != nil {
		t.Errorf("cannot get new dfstore, %v",err)
		return
	}
	defer dfs.Close()

	err = dfs.WriteRecords(dataRows)
	if err != nil {
		t.Errorf("cannot write, %v", err)
	}
	// https://pkg.go.dev/github.com/go-gota/gota/dataframe#DataFrame.Filter

	// TODO compound filters and / or cases
	filters := []dataframe.F{
		dataframe.F{Colname: "artist", Comparator: series.Eq, Comparando: "John Coltrane"},
		dataframe.F{Colname: "price", Comparator: series.Greater, Comparando: "60"},
	}
	res, err := dfs.ReadRecords(filters, 20)
	if err != nil {
		t.Errorf("cannot read, %v", err)
	}
	log.Println("read", res)
}
