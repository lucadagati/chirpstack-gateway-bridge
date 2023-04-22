package api

import (
	"encoding/json"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	log "github.com/sirupsen/logrus"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

var (
	NS_IP_ADDRESS   = "tcp://broker.emqx.io:1883" // Network Server IP address TODO: get parameter (broker_ip_h_ns) through API request
	GWid            = ""                          // Gateway ID TODO: get parameter (gwid_token) through API request
	GWid_TOPIC_NAME = ""                          // Topic name TODO: get parameter (gwid_token) through API request
)

// TODO: implement API listener

// onMessage handles incoming MQTT messages from a broker. It first decodes the message payload and
// logs the details of the received message. It then checks if the payload is a JSON object, modifies it by replacing
// the value of the "gatewayID" key with the value of the GWid variable, and publishes the modified payload to a new topic.
func onMessage(client mqtt.Client, msg mqtt.Message) {
	// Decode payload and print message details
	payload := string(msg.Payload())
	log.Printf("Received message on topic: %s with payload: %s", msg.Topic(), payload)

	// Check if the payload is a JSON object
	var payloadMap map[string]interface{}
	err := json.Unmarshal([]byte(payload), &payloadMap)
	if err != nil {
		log.Println(err)
		return
	}

	modifyMap(payloadMap, "gatewayID", GWid)
	payloadBytes, err := json.Marshal(payloadMap)
	if err != nil {
		log.Println(err)
		return
	}
	payload = string(payloadBytes)

	// Get topic name and type
	topicName := strings.Split(msg.Topic(), "/")[1]
	topicType := strings.Split(msg.Topic(), "/")[len(strings.Split(msg.Topic(), "/"))-1]
	log.Println(msg.Topic())
	newTopic := strings.Replace(msg.Topic(), topicName, GWid_TOPIC_NAME, 1)

	// Handle different topic types
	if topicType == "up" {
		log.Println("event UP")
		payloadMap := make(map[string]interface{})
		err := json.Unmarshal([]byte(payload), &payloadMap)
		if err != nil {
			log.Println(err)
			return
		}
		payloadPHY := payloadMap["phyPayload"].(string)
		decodedPacket := getDevAddr([]byte(payloadPHY))
		log.Println("Decoded packet devAddr:", decodedPacket)
	} else if topicType == "stats" {
		log.Println("event STATS")
	} else if topicType == "conn" {
		log.Println("state CONN")
	}

	// Publish message to NS and print details
	clientOpts := mqtt.NewClientOptions()
	publishClient := mqtt.NewClient(clientOpts)
	if token := publishClient.Connect(); token.Wait() && token.Error() != nil {
		log.Println(token.Error())
		return
	}
	if token := publishClient.Publish(newTopic, 0, false, payload); token.Wait() && token.Error() != nil {
		log.Println(token.Error())
		return
	}
	log.Printf("Forwarded message on topic: %s with payload: %s", newTopic, payload)
	publishClient.Disconnect(0)
}

// subscribeToTopic subscribes an MQTT client to a specific topic
func subscribeToTopic(topic string) {
	// Create a new MQTT client instance
	clientOpts := mqtt.NewClientOptions().AddBroker("tcp://127.0.0.1:1883")
	client := mqtt.NewClient(clientOpts)

	// Connect to the MQTT broker
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	// Subscribe to the specified topic
	if token := client.Subscribe(topic, 0, onMessage); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	// Wait for a signal to exit
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig

	// Unsubscribe from the topic
	client.Unsubscribe(topic)

	// Disconnect from MQTT broker
	client.Disconnect(250)
}

// getDevAddr takes a byte slice phyPayload as input, extracts a specific part of it
// that represents the DevAddr, and returns it in Big Endian format.
func getDevAddr(phyPayload []byte) []byte {
	// Create slices with same length of phyPayload's DevAddr interval
	devAddr := make([]byte, 4)

	// Extract DevAddr from phyPayload
	copy(devAddr, phyPayload[1:5])

	// Reverse bytes order (Little Endian to Big Endian conversion)
	reverse(devAddr)

	return devAddr
}

// reverse takes a slice of any type E and reverses the order of its elements in place by using two pointers i and j
// that start from opposite ends of the slice and swap their corresponding elements until they meet in the middle.
// The function is written using generic type parameters S and E, making it reusable for different types of slices
func reverse[S ~[]E, E any](s S) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}

// modifyMap recursively traverses a map or a slice of maps and modifies the value of a specific key in each nested map.
// It takes three arguments: the payloadMap interface that can hold a map or a slice of maps,
// the key string to search for, and the value string to replace it with.
func modifyMap(payloadMap interface{}, key string, value string) {
	switch m := payloadMap.(type) {
	case map[string]interface{}:
		for k, v := range m {
			if k == key {
				m[k] = value
			} else {
				modifyMap(v, key, value)
			}
		}
	case []interface{}:
		for _, v := range m {
			modifyMap(v, key, value)
		}
	}
}
