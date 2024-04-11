package main

import (
	"errors"
	"log"
	"skabillium/kegdb/cmd/keg"
	"time"

	"github.com/tidwall/resp"
)

func main() {
	keg := keg.NewKegDB()
	err := keg.Open()
	if err != nil {
		panic(err)
	}
	defer keg.Close()

	server := resp.NewServer()

	server.HandleFunc("set", func(conn *resp.Conn, args []resp.Value) bool {
		if len(args) != 3 {
			conn.WriteError(errors.New("ERR wrong number of arguments for 'set' command"))
			return true
		}

		err = keg.Set(args[1].String(), args[2].String())
		if err != nil {
			conn.WriteError(err)
			return true
		}

		conn.WriteSimpleString("OK")
		return true
	})

	server.HandleFunc("get", func(conn *resp.Conn, args []resp.Value) bool {
		if len(args) != 2 {
			conn.WriteError(errors.New("ERR wrong number of arguments for 'get' command"))
			return true
		}

		value, err := keg.Get(args[1].String())
		if err != nil {
			conn.WriteError(err)
			return true
		}

		conn.WriteString(value)

		return true
	})

	server.HandleFunc("del", func(conn *resp.Conn, args []resp.Value) bool {
		if len(args) != 2 {
			conn.WriteError(errors.New("ERR wrong number of arguments for 'get' command"))
			return true
		}
		return true
	})

	server.HandleFunc("quit", func(conn *resp.Conn, args []resp.Value) bool {
		return false
	})

	go keg.RunSnapshotJob(1 * time.Minute)

	if err := server.ListenAndServe(":5678"); err != nil {
		log.Fatal(err)
	}
}
