package main

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/IBM/sarama"
)

type Order struct {
	CustomerName string `json:"customer_name"`
	CoffeeType   string `json:"coffee_type"`
}

func main() {
	http.HandleFunc("POST /order", handleOrder)
	http.ListenAndServe(":1333", nil)
}

func ConnectProducer(brokers []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 3

	return sarama.NewSyncProducer(brokers, config)
}

func PushToQueue(topic string, message []byte) error {
	brokers := []string{"localhost:9092"}

	producer, err := ConnectProducer(brokers)
	if err != nil {
		return err
	}
	defer producer.Close()

	partition, offset, err := producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	})
	if err != nil {
		return err
	}

	log.Printf("Order is stored in topic(%s)/partition(%d)/offset(%d)\n",
		topic,
		partition,
		offset)

	return nil
}

func handleOrder(w http.ResponseWriter, r *http.Request) {
	var orderReq Order
	err := json.NewDecoder(r.Body).Decode(&orderReq)
	if err != nil {
		log.Println(err)
		http.Error(w, "error decoding the request body", http.StatusBadRequest)
		return
	}

	dataBytes, err := json.Marshal(orderReq)
	if err != nil {
		log.Println(err)
		http.Error(w, "error marshalling the json value", http.StatusInternalServerError)
		return
	}

	err = PushToQueue("coffee_orders", dataBytes)
	if err != nil {
		log.Println(err)
		http.Error(w, "error pushing the message to the coffee_order topic", http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"success": true,
		"msg":     "Order for " + orderReq.CustomerName + " placed successfully!",
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Println(err)
		http.Error(w, "error encoding the json response", http.StatusInternalServerError)
		return
	}
}
