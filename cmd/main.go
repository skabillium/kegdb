package main

import (
	"errors"
	"fmt"
	"os"
	"skabillium/kegdb/cmd/keg"
	"time"

	"github.com/tidwall/resp"
)

type Server struct {
	port string

	srv *resp.Server
	db  *keg.Keg
}

func NewServer(port string) *Server {
	return &Server{
		port: port,
		srv:  resp.NewServer(),
		db:   keg.NewKegDB(),
	}
}

func (s *Server) registerHandlers() {
	s.srv.HandleFunc("info", func(conn *resp.Conn, args []resp.Value) bool {
		conn.WriteSimpleString("KegDB version 0.0.1")
		return true
	})

	s.srv.HandleFunc("reindex", func(conn *resp.Conn, args []resp.Value) bool {
		conn.WriteSimpleString("TODO: Implement 'reindex' command")
		return true
	})

	s.srv.HandleFunc("set", func(conn *resp.Conn, args []resp.Value) bool {
		if len(args) != 3 {
			conn.WriteError(errors.New("ERR wrong number of arguments for 'set' command"))
			return true
		}

		err := s.db.Put(args[1].String(), args[2].String())
		if err != nil {
			conn.WriteError(err)
			return true
		}

		conn.WriteSimpleString("OK")
		return true
	})

	s.srv.HandleFunc("get", func(conn *resp.Conn, args []resp.Value) bool {
		if len(args) != 2 {
			conn.WriteError(errors.New("ERR wrong number of arguments for 'get' command"))
			return true
		}

		value, found, err := s.db.Get(args[1].String())
		if err != nil {
			conn.WriteError(err)
			return true
		}

		if !found {
			conn.WriteNull()
			return true
		}

		conn.WriteString(value)
		return true
	})

	s.srv.HandleFunc("del", func(conn *resp.Conn, args []resp.Value) bool {
		if len(args) != 2 {
			conn.WriteError(errors.New("ERR wrong number of arguments for 'get' command"))
			return true
		}

		deleted, err := s.db.Delete(args[1].String())
		if err != nil {
			conn.WriteError(err)
			return true
		}

		var out int
		if deleted {
			out = 1
		}

		conn.WriteInteger(out)
		return true
	})

	s.srv.HandleFunc("quit", func(conn *resp.Conn, args []resp.Value) bool {
		return false
	})
}

func (s *Server) Start() error {
	err := s.db.Open()
	if err != nil {
		return err
	}
	defer s.db.Close()

	s.registerHandlers()

	go s.db.RunSnapshotJob(1 * time.Minute)
	fmt.Println("Started snapshot job")

	fmt.Println("KeyDB server started at port:", s.port)
	if err = s.srv.ListenAndServe(":" + s.port); err != nil {
		return err
	}

	return nil
}

func main() {
	server := NewServer("5678")

	err := server.Start()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
