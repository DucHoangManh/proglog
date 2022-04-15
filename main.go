package main

import (
	"github.com/DucHoangManh/proglog/internal/server"
	"log"
)

func main() {
	srv := server.NewHttpServer(":8080")
	log.Printf("server running at :8080")
	log.Fatalln(srv.ListenAndServe())
}
