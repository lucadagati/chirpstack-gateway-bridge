package api

import (
	"encoding/json"
	"fmt"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	log "github.com/sirupsen/logrus"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

type Body struct {
	AddedBroker string `json:"added_broker"`
	BrokerIPHNS string `json:"broker_ip_h_ns"`
	GWIDToken   string `json:"gwid_token"`
}

var (
	NsIpAddress   = ""     // (Home) Network Server IP address
	AddedBroker   = ""     // Added broker IP address
	GWid          = ""     // Gateway ID
	GwidTopicName = ""     // Topic name
	port          = "3000" // API listener port
)

// Launch starts the API
func Launch() func() error {
	return func() error {
		go startListener(port)
		return nil
	}
}

// startListener start a Listener on specified port to serve requests
func startListener(port string) {
	http.HandleFunc("/", handleRequest)
	log.Printf("API listening on port %s...\n", port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", port), nil))
}

// handleRequest handles API POST requests, taking a JSON object with three parameters:
// added_broker (AddedBroker), broker_ip_h_ns (BrokerIPHNS), gwid_token (GWIDToken)
func handleRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	var b Body
	if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	log.Printf("Received request with added_broker=%s, broker_ip_h_ns=%s, gwid_token=%s\n",
		b.AddedBroker, b.BrokerIPHNS, b.GWIDToken)

	// Assignment
	NsIpAddress = b.BrokerIPHNS
	AddedBroker = b.AddedBroker
	GWid = b.GWIDToken
	GwidTopicName = b.GWIDToken
	log.WithFields(log.Fields{
		"ip":    NsIpAddress,
		"gwid":  GWid,
		"topic": GwidTopicName,
	}).Info("IP: " + NsIpAddress + "\nGWid: " + GWid + "\nTopic name: " + GwidTopicName)

	// TODO: Handle request here

	var newline []byte
	newline = []byte("\n")
	response := map[string]string{
		"status": "ok",
	}
	jsonResponse, err := json.Marshal(response)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonResponse = append(jsonResponse, newline...)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(jsonResponse)

	go subscribeToTopic("gateway/+/event/up")
	go subscribeToTopic("gateway/+/event/stats")
	go subscribeToTopic("gateway/+/state/conn")
}

// onMessage handles incoming MQTT messages from a broker. It first decodes the message payload and
// logs the details of the received message. It then checks if the payload is a JSON object, modifies it by replacing
// the value of the "gatewayID" key with the value of the GWid variable, and publishes the modified payload to a new topic.
func onMessage(client mqtt.Client, msg mqtt.Message) {
	// Decode payload and print message details
	payload := string(msg.Payload())

	// Check if the payload is a JSON object
	var payloadMap map[string]interface{}
	if err := json.Unmarshal([]byte(payload), &payloadMap); err != nil {
		log.WithError(err).Warn("Failed to parse payload as JSON")
	} else {
		if len(payload) > 0 {
			log.WithFields(log.Fields{
				"package": "mqtt",
				"topic":   msg.Topic(),
				"payload": payload,
			}).Info("Received message on topic: " + msg.Topic() + " with payload: " + payload)
		}
	}

	// Replace gatewayID with GWid received through API
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
	newTopic := strings.Replace(msg.Topic(), topicName, GwidTopicName, 1)

	// Handle different topic types
	if topicType == "up" {
		log.WithFields(log.Fields{
			"package": "mqtt",
			"topic":   msg.Topic(),
			"payload": payload,
		}).Info("Handling event UP")
		payloadMap = make(map[string]interface{})
		if err = json.Unmarshal([]byte(payload), &payloadMap); err != nil {
			log.WithFields(log.Fields{
				"package": "mqtt",
				"topic":   msg.Topic(),
				"payload": payload,
				"error":   err.Error(),
			}).Error("Failed to decode LoRa packet")
			return
		}
		if payloadMap["phyPayload"] != nil {
			payloadPHY := payloadMap["phyPayload"].(string)
			decodedPacket := getDevAddr([]byte(payloadPHY))
			log.Println("Decoded packet devAddr:", decodedPacket)
		}
	} else if topicType == "stats" {
		log.WithFields(log.Fields{
			"package": "mqtt",
			"topic":   msg.Topic(),
			"payload": payload,
		}).Info("Handling event STATS")
		// TODO: handle stats event
	} else if topicType == "conn" {
		log.WithFields(log.Fields{
			"package": "mqtt",
			"topic":   msg.Topic(),
			"payload": payload,
		}).Info("Handling state CONN")
		// TODO: handle connection state event
	} else {
		log.WithFields(log.Fields{
			"package": "mqtt",
			"topic":   msg.Topic(),
			"payload": payload,
		}).Warn("Unknown topic type")
	}

	// Publish message to NS and print details
	publishOpts := mqtt.NewClientOptions().AddBroker(NsIpAddress).SetClientID("MQTT_Forwarder_Target")
	publishClient := mqtt.NewClient(publishOpts)
	if token := publishClient.Connect(); token.Wait() && token.Error() != nil {
		log.WithFields(log.Fields{
			"package": "mqtt",
			"topic":   msg.Topic(),
			"error":   token.Error(),
		}).Error("Failed to connect to NS broker")
		return
	}
	if token := publishClient.Publish(newTopic, 0, false, payload); token.Wait() && token.Error() != nil {
		log.WithFields(log.Fields{
			"package": "mqtt",
			"topic":   msg.Topic(),
			"payload": payload,
			"error":   token.Error(),
		}).Error("Failed to publish message to NS broker")
		return
	}

	log.WithFields(log.Fields{
		"package": "mqtt",
		"topic":   msg.Topic(),
		"payload": payload,
	}).Info("Forwarded message on topic: " + newTopic + " with payload: " + payload)

	publishClient.Disconnect(250)
}

// subscribeToTopic subscribes an MQTT client to a specific topic
func subscribeToTopic(topic string) {
	// Define new MQTT client options
	clientOpts := mqtt.NewClientOptions().AddBroker(AddedBroker).SetClientID("MQTT_Forwarder")
	clientOpts.SetDefaultPublishHandler(onMessage)

	// Create a new MQTT client instance
	client := mqtt.NewClient(clientOpts)

	// Connect to the MQTT broker
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.WithError(token.Error()).Fatal("error connecting to MQTT broker")
	}

	// Subscribe to the specified topic
	if token := client.Subscribe(topic, 0, nil); token.Wait() && token.Error() != nil {
		log.WithError(token.Error()).Fatal("error subscribing to topic")
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
