package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/childoftheuniverse/red-cloud"
	"github.com/childoftheuniverse/red-cloud/client"
	"github.com/golang/protobuf/proto"
)

/*
Get fetches a column from the specified table / column family / key and
outputs the text protocol buffer to stdout.
*/
func (c *RedCloudCLI) Get(
	ctx context.Context, tableSpec, columnFamily, column, key string) {
	var dac *client.DataAccessClient
	var req *redcloud.GetRequest
	var col *redcloud.Column
	var instance, table string
	var err error

	if instance, table, err = client.SplitTablePath(tableSpec); err != nil {
		log.Fatalf("Invalid table specification: %s: %s", tableSpec, err)
	}

	req = &redcloud.GetRequest{
		Key:          []byte(key),
		Table:        table,
		ColumnFamily: columnFamily,
		Column:       column,
	}

	dac = client.NewDataAccessClient(instance, c.etcdClient, c.tlsConfig)

	if col, err = dac.Get(ctx, req); err != nil {
		log.Fatalf("Error requesting column %s:%s: %s", columnFamily, column,
			err)
	}

	fmt.Print(proto.MarshalTextString(col))
}

/*
printResponses formats responses of type ColumnSet and outputs them to
the standard output channel.
*/
func printResponses(colsets chan *redcloud.ColumnSet, cancel chan bool) {
	var colset *redcloud.ColumnSet
	for {
		select {
		case colset = <-colsets:
			fmt.Print(proto.MarshalTextString(colset))
		case <-cancel:
			return
		}
	}
}

/*
GetRange fetches all specified columns from the specified
table / column family / key and outputs the text protocol buffer to stdout.
*/
func (c *RedCloudCLI) GetRange(
	ctx context.Context, tableSpec, columnFamily string, columns []string,
	startkey, endkey string) {
	var resp = make(chan *redcloud.ColumnSet)
	var cancel = make(chan bool)
	var dac *client.DataAccessClient
	var req *redcloud.GetRangeRequest
	var instance, table string
	var err error

	if instance, table, err = client.SplitTablePath(tableSpec); err != nil {
		log.Fatalf("Invalid table specification: %s: %s", tableSpec, err)
	}

	req = &redcloud.GetRangeRequest{
		StartKey:     []byte(startkey),
		EndKey:       []byte(endkey),
		Table:        table,
		ColumnFamily: columnFamily,
		Column:       columns,
	}

	dac = client.NewDataAccessClient(instance, c.etcdClient, c.tlsConfig)

	go printResponses(resp, cancel)

	if err = dac.GetRange(ctx, req, resp); err != nil && err != io.EOF {
		log.Fatalf("Error requesting column %s:%v: %s", columnFamily, columns,
			err)
	}

	cancel <- true
}

/*
Insert creates a new column and inserts it into the specified
table / column family / key.
*/
func (c *RedCloudCLI) Insert(
	ctx context.Context, tableSpec, columnFamily, column, key, data string) {
	var dac *client.DataAccessClient
	var req *redcloud.InsertRequest
	var instance, table string
	var err error

	if instance, table, err = client.SplitTablePath(tableSpec); err != nil {
		log.Fatalf("Invalid table specification: %s: %s", tableSpec, err)
	}

	req = &redcloud.InsertRequest{
		Key:          []byte(key),
		Table:        table,
		ColumnFamily: columnFamily,
		ColumnName:   column,
		Column: &redcloud.Column{
			Type:      redcloud.Column_DATA,
			Timestamp: time.Now().UnixNano() / 1000000,
			Content:   []byte(data),
		},
	}

	dac = client.NewDataAccessClient(instance, c.etcdClient, c.tlsConfig)

	if err = dac.Insert(ctx, req); err != nil {
		log.Fatalf("Error requesting column %s:%s: %s", columnFamily, column,
			err)
	}
}
