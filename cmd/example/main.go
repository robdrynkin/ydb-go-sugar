package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/google/uuid"
	"github.com/robdrynkin/ydb-go-sugar/pkg/helpers"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"log"
	"os"
)

var endpoint string
var database string
var tableName string
var secure bool

func ParseArgs() {
	flag.StringVar(&endpoint, "endpoint", "localhost:2136", "YDB endpoint")
	flag.StringVar(&database, "database", "local", "YDB database")
	flag.StringVar(&tableName, "table", "s3_test", "YDB table")
	flag.BoolVar(&secure, "secure", false, "Use secure connection")
	flag.Parse()
}

func ConnectToDb(ctx context.Context) *ydb.Driver {
	token := os.Getenv("YDB_TOKEN")
	var ydbOptions []ydb.Option
	if token != "" {
		ydbOptions = append(ydbOptions, ydb.WithAccessTokenCredentials(token))
	}
	db, err := ydb.Open(ctx, sugar.DSN(endpoint, database), ydbOptions...)
	if err != nil {
		log.Fatal("db connection error", err)
	}
	return db
}

type Data struct {
	BlobId   uuid.UUID `ydb:"blob_id,uuid,pk"`
	ChunkNum int32     `ydb:"chunk_num,int32,pk"`
	Data     []byte    `ydb:"data,string"`
}

func main() {
	ctx := context.Background()

	ParseArgs()
	db := ConnectToDb(ctx)

	shugar, err := helpers.NewTableSugar[Data](tableName)
	if err != nil {
		log.Fatal("sugar error", err)
		return
	}

	shugar.DropTable(ctx, db.Scripting())
	err = shugar.CreateTable(ctx, db.Scripting(), 10)
	if err != nil {
		log.Fatal("table creation error", err)
		return
	}

	k1 := uuid.New()
	k2 := uuid.New()
	blobs := []Data{
		{BlobId: k1, ChunkNum: 1, Data: []byte("data1")},
		{BlobId: k1, ChunkNum: 2, Data: []byte("data2")},
		{BlobId: k2, ChunkNum: 1, Data: []byte("data3")},
	}

	err = shugar.BulkUpsert(ctx, db.Table(), database, blobs)
	if err != nil {
		log.Fatal("bulk upsert error: ", err)
		return
	}

	key := Data{BlobId: k1, ChunkNum: 1}
	var row *Data
	err = db.Table().Do(ctx, func(ctx context.Context, session table.Session) error {
		readTx := table.TxControl(table.BeginTx(table.WithOnlineReadOnly(table.WithInconsistentReads())), table.CommitTx())
		row, err = shugar.SelectByPK(ctx, session, readTx, &key)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		log.Fatal("select by pk error: ", err)
	}
	if row == nil {
		log.Fatal("row is nil")
	}
	fmt.Println(string(row.Data))
}
