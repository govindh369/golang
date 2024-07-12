package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/segmentio/kafka-go"
	"github.com/streadway/amqp"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Credentials represents login credentials.
type Credentials struct {
	Username string    `json:"username"`
	Password string    `json:"password"`
	CurTime  time.Time `json:"curtime"`
}

// SignupRequest represents signup request data.
type SignupRequest struct {
	Username   string    `json:"username"`
	Password   string    `json:"password"`
	Email      string    `json:"email"`
	Timestamp  time.Time `json:"timestamp"`
	KeyPress   string    `json:"keyPress,omitempty"`
	ClickCoord struct {
		X int `json:"x"`
		Y int `json:"y"`
	} `json:"clickCoordinates,omitempty"`

	ScrollEvent    int
	ConnectionInfo struct {
		RTT           float64 `json:"RTT"`
		Type          string  `json:"type"`
		Downlink      float64 `json:"downlink"`
		EffectiveType string  `json:"effectiveType"`
	} `json:"connectionInfo,omitempty"`
}

// ResponseMessage represents the structure of the response JSON data.
type ResponseMessage struct {
	Message string `json:"message"`
}

type KafkaProducer struct {
	Writer *kafka.Writer
}

type KafkaConsumer struct {
	Reader *kafka.Reader
}

// var client *mongo.Client
var signupCollection *mongo.Collection
var loginCollection *mongo.Collection
var redisClient *redis.Client

var rabbitMQChannelGlobal *amqp.Channel

var kafkaProducer *KafkaProducer
var kafkaConsumer *KafkaConsumer

func init() {
	clientOptions := options.Client().ApplyURI("mongodb://localhost:27017/")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		log.Fatal(err)
	}

	err = client.Ping(ctx, nil)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Connected to MongoDB")

	redisClient = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	_, err = redisClient.Ping(ctx).Result()
	if err != nil {
		log.Fatal("Error connecting to Redis:", err)
	}

	fmt.Println("Connected to Redis")

	database := client.Database("loginform")
	signupCollection = database.Collection("signupdetails")
	loginCollection = database.Collection("logindetails")

	kafkaProducer = &KafkaProducer{
		Writer: kafka.NewWriter(kafka.WriterConfig{
			Brokers:  []string{"localhost:9093"},
			Topic:    "my-kafka-topic",
			Balancer: &kafka.LeastBytes{},
		}),
	}

	kafkaConsumer = &KafkaConsumer{
		Reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:   []string{"localhost:9093"},
			Topic:     "my-kafka-topic",
			Partition: 0,
			MinBytes:  10e3, // 10KB
			MaxBytes:  10e6, // 10MB
		}),
	}

}

func initRabbitMQ() (*amqp.Channel, error) {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		return nil, err
	}

	rabbitMQChannel, err := conn.Channel()
	if err != nil {
		log.Printf("Error creating RabbitMQ channel: %v", err)
		return nil, err
	}

	_, err = rabbitMQChannel.QueueDeclare(
		"login",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Printf("Error declaring login queue: %v", err)
		return nil, err
	}

	log.Println("Connected to RabbitMQ successfully")

	rabbitMQChannelGlobal = rabbitMQChannel

	return rabbitMQChannel, nil
}

func main() {

	app := fiber.New()
	app.Use(cors.New())

	rabbitMQChannel, err := initRabbitMQ()
	if err != nil {
		log.Fatal("Error initializing RabbitMQ:", err)
	}
	defer rabbitMQChannel.Close()

	app.Post("/login", func(c *fiber.Ctx) error {

		username := c.FormValue("username")
		if _, err := getCredentialsFromCache(username); err == nil {
			return c.SendString("Login Successful. Redirecting to home page.")
		}

		var Logincredentials Credentials
		if err := c.BodyParser(&Logincredentials); err != nil {
			return c.Status(http.StatusBadRequest).SendString("Invalid JSON format")
		}

		if Logincredentials.Username == "" || Logincredentials.Password == "" {
			return c.Status(http.StatusBadRequest).SendString("Please enter both username and password")
		}

		checkAndStoreCredentials(Logincredentials)

		if err := storeCredentialsInCache(Logincredentials.Username, "login"); err != nil {

			log.Println("error storing login credentials in the redis cache", err)

		}

		publishToKafka("login", Logincredentials)

		return c.SendString("Login Successful. Redirecting to home page.")

	})

	app.Post("/signup", func(c *fiber.Ctx) error {

		username := c.FormValue("username")
		if _, err := getSignupDataFromCache(username); err == nil {
			return c.SendString("Signup data found in cache.")
		}

		var request SignupRequest
		if err := c.BodyParser(&request); err != nil {
			// fmt.Println(err)
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Error parsing JSON"})
		}
		// fmt.Println(c)

		err := saveUser(request)
		if err != nil {
			return c.Status(http.StatusInternalServerError).SendString("Error saving user")
		}

		err = storeSignupDataInCache(request.Username)

		if err != nil {
			log.Println("Error storing the data in redis cache :", err)
		}

		publishToKafka("signup", request)

		return c.JSON(map[string]string{"message": "Signup successful"})
	})

	go consumeSignupData()
	go consumeLoginData()

	log.Fatal(app.Listen(":2411"))
}

func checkAndStoreCredentials(credentials Credentials) {
	credentials.CurTime = time.Now()

	_, err := loginCollection.InsertOne(context.TODO(), credentials)
	if err != nil {
		log.Println("Error storing login credentials:", err)
		return
	}

	fmt.Printf("Stored login credentials for user: %s\n", credentials.Username)

	publishToRabbitMQ("login", credentials)
}

func saveUser(request SignupRequest) error {
	request.Timestamp = time.Now()
	_, err := signupCollection.InsertOne(context.TODO(), request)
	if err != nil {
		return err
	}

	return nil
}

func storeCredentialsInCache(username, dataType string) error {
	key := getKeyForCache(username, dataType)
	err := redisClient.Set(context.Background(), key, "true", 24*time.Hour).Err()
	if err != nil {
		log.Printf("Error storing %s credentials in Redis cache for user %s: %v", dataType, username, err)
		return err
	}
	return nil
}

func getCredentialsFromCache(username string) (string, error) {
	// Check if the username exists in Redis cache
	return redisClient.Get(context.Background(), getKeyForCache(username, "login")).Result()
}

func storeSignupDataInCache(username string) error {
	key := getKeyForCache(username, "signup")
	err := redisClient.Set(context.Background(), key, "true", 24*time.Hour).Err()
	if err != nil {
		log.Printf("Error storing signup data in Redis cache for user %s: %v", username, err)
		return err
	}
	return nil
}

func getSignupDataFromCache(username string) (string, error) {
	// Check if the username exists in Redis cache
	return redisClient.Get(context.Background(), getKeyForCache(username, "signup")).Result()
}

func getKeyForCache(username, dataType string) string {
	return fmt.Sprintf("%s:%s", dataType, username)
}

func publishToRabbitMQ(dataType string, data interface{}) error {

	body, err := json.Marshal(data)
	if err != nil {
		return err
	}

	err = rabbitMQChannelGlobal.Publish(
		"",
		dataType,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)
	if err != nil {
		fmt.Println("publish error:", err)
	}

	log.Printf("Publishing message using Rabbitmq: %s", body)

	return err
}

func publishToKafka(dataType string, data interface{}) {
	body, err := json.Marshal(data)
	if err != nil {
		log.Println("Error marshaling data:", err)
		return
	}

	msg := kafka.Message{
		Key:   []byte(dataType),
		Value: body,
	}

	err = kafkaProducer.Writer.WriteMessages(context.Background(), msg)
	if err != nil {
		log.Println("Error publishing message to Kafka:", err)
	}
	fmt.Printf("send signup data from kafka: %+v\n", data)

}

func consumeSignupData() {
	for {
		msg, err := kafkaConsumer.Reader.ReadMessage(context.Background())
		if err != nil {
			log.Println("Error reading message from Kafka:", err)
			continue
		}

		var request SignupRequest
		err = json.Unmarshal(msg.Value, &request)
		if err != nil {
			log.Println("Error unmarshaling Kafka message:", err)
			continue
		}

		// Process the signup data here
		fmt.Printf("Received signup data from kafka: %+v\n", request)
	}
}

func consumeLoginData() {
	msgs, err := rabbitMQChannelGlobal.Consume(
		"login", // Queue name
		"",      // Consumer name, empty string means generate a unique name
		true,    // Auto-Acknowledge, messages will be automatically acknowledged
		false,   // Exclusive, exclusive consumer (only this connection can consume)
		false,   // NoLocal, do not deliver our own messages
		false,   // NoWait, if the server cannot consume immediately, do not wait
		nil,     // Arguments
	)
	if err != nil {
		log.Fatalf("Error consuming messages from RabbitMQ: %v", err)
		return
	}

	for msg := range msgs {
		var loginCredentials Credentials
		err := json.Unmarshal(msg.Body, &loginCredentials)
		if err != nil {
			log.Println("Error unmarshaling RabbitMQ message:", err)
			continue
		}

		// Process the login data here
		fmt.Printf("Received login data from RabbitMQ: %+v\n", loginCredentials)
	}
}
