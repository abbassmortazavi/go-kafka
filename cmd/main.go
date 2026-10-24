package main

import (
	"flag"
	"fmt"
	"go-kafka/kafka"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/cors"
	"github.com/gofiber/fiber/v3/middleware/logger"
	recover2 "github.com/gofiber/fiber/v3/middleware/recover"
)

func main() {
	nodID := flag.String("node", "node-1", "not node ID for this instance")
	port := flag.String("port", "3000", "port to listen on")
	kafkaAddress := flag.String("kafka", "127.0.0.1:29092", "kafka address")
	flag.Parse()

	//wait for kafka to ready
	if err := kafka.WaitForKafka(*kafkaAddress, 30*time.Second); err != nil {
		log.Fatalf("kafka wait for kafka failed: %v", err)
	}
	app := fiber.New(fiber.Config{
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	})

	app.Use(logger.New())
	app.Use(recover2.New())
	app.Use(cors.New(cors.Config{
		AllowOrigins: []string{"*"},
		AllowHeaders: []string{"Origin", "Accept", "Content-Type", "Content-Length", "Accept-Encoding", "X-CSRF-Token"},
	}))

	//Create Heartbeat Manager
	heartbeatManager, err := kafka.NewHeartBeatManager(*kafkaAddress, *nodID)
	if err != nil {
		log.Fatal(err)
	}

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGALRM)

	go func() {
		sig := <-sigchan
		fmt.Println("shutting down on signal", sig)
		heartbeatManager.Stop()
		err := app.ShutdownWithTimeout(10 * time.Second)
		if err != nil {
			return
		}

	}()
	log.Printf("listening on port %s", *port)

	if err := app.Listen(":" + *port); err != nil {
		log.Fatal(err)
	}
}

//section 6=>12 finished
