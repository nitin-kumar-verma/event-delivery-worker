package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/gofiber/fiber/v2"
	"github.com/joho/godotenv"
)

type Event struct {
	UserID  string `json:"userId"`
	Payload string `json:"payload"`
}

const (
	EVENT_KEY      = "queued_events"
	PROCESSING_KEY = "processing_events"
)

func main() {
	app := fiber.New()
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
	client := redisClient()
	defer client.Close()

	//Invoke delivery worker on a goroutine
	go deliveryWorker()

	err = app.Listen(":3001")
	if err != nil {
		panic(err)
	}
}

func deliveryWorker() {
	client := redisClient()
	defer client.Close()
	//code sleeps for 100ms before polling redis again as too many redis requests can bring the system down.
	//Ideal replacement for this issue is a publisher-subsciber based solution eg Kafka or Azure Service bus or Amazon SQS
	for {
		ctx := context.Background()
		txPipeline := client.TxPipeline()
		rpopResult := txPipeline.RPopLPush(ctx, EVENT_KEY, PROCESSING_KEY)
		_, err := txPipeline.Exec(ctx)
		if err != nil {
			//No values present in redis. so continue
			time.Sleep(100 * time.Millisecond)
			continue
		} else if rpopResult.Err() != nil {
			//Log error and continue
			time.Sleep(100 * time.Millisecond)
			continue
		}

		var event Event

		json.Unmarshal([]byte(rpopResult.Val()), &event)

		//fanout requests in a goroutine
		go fanOutRequests(event, ctx, client, *rpopResult)

		time.Sleep(100 * time.Millisecond)
	}

}

func fanOutRequests(event Event, ctx context.Context, client *redis.Client, rpopResult redis.StringCmd) {
	hitEndpointWithBackoff("https://event-delivery-dest-1.deno.dev/", event)
	hitEndpointWithBackoff("https://event-delivery-dest-2.deno.dev/", event)
	hitEndpointWithBackoff("https://event-delivery-dest-3.deno.dev/", event)

	//Delete the event from processing queue be it processed or retries max number of times
	defer func() {
		txPipeline := client.TxPipeline()
		txPipeline.LRem(ctx, PROCESSING_KEY, 0, rpopResult.Val())

		_, err := txPipeline.Exec(ctx)
		if err != nil {

			//Error while deleting key from redis needs to be reported and dev intervention might be required
		}

	}()

}

func hitEndpointWithBackoff(endpoint string, event Event) {
	//Max Retries can be configured through env if required
	maxRetries := 5
	retry := 0
	for {
		err := hitEndpoint(endpoint)
		if err == nil {
			return // success scenario
		}

		retry++
		//Log and handle err based on usecase basis
		if retry > maxRetries {
			return // Max retries reached
		}

		backoffDuration := getBackoffDuration(retry)
		//Sleep for above duration
		time.Sleep(backoffDuration)
	}
}

func hitEndpoint(endpoint string) error {
	resp, err := http.Get(endpoint)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP request failed with status code: %d", resp.StatusCode)
	}

	return nil
}

func getBackoffDuration(retry int) time.Duration {
	// Exponential backoff formula: backoff = base * 2^retry
	base := 1 * time.Second
	return base * (1 << retry)
}

func redisClient() *redis.Client {

	redisAddr, redisAddrExists := os.LookupEnv("REDIS_ADDR")
	redisPassword, redisPassExists := os.LookupEnv("REDIS_PASSWORD")
	redisDB, redisDBExists := os.LookupEnv("REDIS_DB")
	if !(redisAddrExists || redisPassExists || redisDBExists) {
		panic("redis envs not set properly")
	}

	db, err := strconv.Atoi(redisDB)
	if err != nil {
		panic(err)
	}
	client := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: redisPassword,
		DB:       db,
	})
	return client
}
