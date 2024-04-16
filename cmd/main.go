package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"skabillium/kegdb/cmd/keg"
	"time"

	"github.com/tidwall/resp"
)

type Server struct {
	port          string
	mergeInterval time.Duration

	srv *resp.Server
	db  *keg.Keg
	log *log.Logger
}

func NewServer(opts *ServerOptions) *Server {
	return &Server{
		port:          opts.Port,
		mergeInterval: opts.MergeInterval,
		srv:           resp.NewServer(),
		db:            keg.NewKegDB(keg.KegOptions{DataDir: opts.DataDir}),
		log:           log.New(os.Stderr, "", log.Ldate|log.Ltime),
	}
}

func (s *Server) registerHandlers() {
	s.srv.HandleFunc("info", func(conn *resp.Conn, args []resp.Value) bool {
		conn.WriteSimpleString("KegDB version 0.0.1")
		return true
	})

	s.srv.HandleFunc("index", func(conn *resp.Conn, args []resp.Value) bool {
		err := s.db.Index()
		if err != nil {
			conn.WriteError(err)
			return true
		}
		conn.WriteSimpleString("OK")
		return true
	})

	s.srv.HandleFunc("merge", func(conn *resp.Conn, args []resp.Value) bool {
		err := s.db.Merge()
		if err != nil {
			conn.WriteError(err)
			return true
		}
		conn.WriteSimpleString("OK")
		return true
	})

	s.srv.HandleFunc("keys", func(conn *resp.Conn, args []resp.Value) bool {
		keys := s.db.Keys()
		values := []resp.Value{}
		for i := 0; i < len(keys); i++ {
			values = append(values, resp.StringValue(keys[i]))
		}

		conn.WriteArray(values)
		return true
	})

	s.srv.HandleFunc("put", func(conn *resp.Conn, args []resp.Value) bool {
		if len(args) != 3 {
			conn.WriteError(errors.New("ERR wrong number of arguments for 'put' command"))
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

func (s *Server) RunMergeJob(interval time.Duration) {
	ticker := time.NewTicker(interval)
	for range ticker.C {
		s.db.Merge()
	}
}

func (s *Server) Start() error {
	err := s.db.Open()
	if err != nil {
		return err
	}
	defer s.db.Close()

	s.registerHandlers()

	go s.RunMergeJob(s.mergeInterval)
	s.log.Println("Started merge job")

	// go s.db.RunSnapshotJob(1 * time.Minute)
	// s.log.Println("Started snapshot job")

	s.log.Println("KeyDB server started at port:", s.port)
	if err = s.srv.ListenAndServe(":" + s.port); err != nil {
		return err
	}

	return nil
}

type ServerOptions struct {
	Port          string
	DataDir       string
	MergeInterval time.Duration
}

func getServerOptions() *ServerOptions {
	options := &ServerOptions{
		Port:          "5678",
		DataDir:       "data",
		MergeInterval: 24 * time.Hour,
	}

	var (
		port               string
		dataDir            string
		mergeIntervalHours int
	)

	flag.StringVar(&port, "port", "", "Port to run server")
	flag.StringVar(&dataDir, "dir", "", "Directory to save data")
	flag.IntVar(&mergeIntervalHours, "merge-interval", -1, "Interval to run merge job in hours")
	flag.Parse()

	if port != "" {
		options.Port = port
	}

	if dataDir != "" {
		options.DataDir = dataDir
	}

	if mergeIntervalHours != -1 {
		options.MergeInterval = time.Duration(mergeIntervalHours * int(time.Hour))
	}

	return options
}

func main() {
	opts := getServerOptions()
	server := NewServer(opts)

	err := server.Start()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
