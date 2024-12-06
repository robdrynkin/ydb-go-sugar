package reflection

import (
	"strings"
	"testing"
)

type Book struct {
	Id     uint64 `ydb:"id,uint64,pk"`
	Title  []byte `ydb:"title,string"`
	Author []byte `ydb:"author,string"`
}

func TestNewSugar(t *testing.T) {
	s, err := NewSugar[Book]()
	if err != nil {
		t.Fatal(err)
	}
	if len(s.Columns) != 3 {
		t.Fatal(s)
	}
	if s.Columns[0].Name != "id" {
		t.Fatal(s)
	}
	if s.Columns[0].Type != "uint64" {
		t.Fatal(s)
	}
	if s.Columns[0].StructFiledName != "Id" {
		t.Fatal(s)
	}
	if s.Columns[1].Name != "title" {
		t.Fatal(s)
	}
	if s.Columns[1].Type != "string" {
		t.Fatal(s)
	}
	if s.Columns[1].StructFiledName != "Title" {
		t.Fatal(s)
	}
	if s.Columns[2].Name != "author" {
		t.Fatal(s)
	}
	if s.Columns[2].Type != "string" {
		t.Fatal(s)
	}
	if s.Columns[2].StructFiledName != "Author" {
		t.Fatal(s)
	}
	if len(s.PrimaryKeys) != 1 {
		t.Fatal(s)
	}
	if s.PrimaryKeys[0].Name != "id" {
		t.Fatal(s)
	}
}

func TestSugar_ToStructValue(t *testing.T) {
	c, err := NewSugar[Book]()
	if err != nil {
		t.Fatal(err)
	}
	book := Book{Id: 3, Title: []byte("The Great Gatsby"), Author: []byte("F")}
	val, err := c.ToStructValue(&book)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(val.Yql(), "`author`:\"F\"") {
		t.Fatal(val)
	}
	if !strings.Contains(val.Yql(), "`id`:3") {
		t.Fatal(val)
	}
	if !strings.Contains(val.Yql(), "`title`:\"The Great Gatsby\"") {
		t.Fatal(val)
	}
}

func TestShugar_MakeReadValue(t *testing.T) {
	c, err := NewSugar[Book]()
	if err != nil {
		t.Fatal(err)
	}
	book := Book{}
	vals := c.MakeReadValue(&book)
	if len(vals) != 3 {
		t.Fatal(vals)
	}
	if vals[0].Name != "id" {
		t.Fatal(vals)
	}
	if vals[1].Name != "title" {
		t.Fatal(vals)
	}
	if vals[2].Name != "author" {
		t.Fatal(vals)
	}
}
