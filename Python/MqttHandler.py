import paho.mqtt.client as mqtt
import ssl

class MQTTHandler:
    def __init__(self, broker, port, topics=None, username=None, password=None):
        """
        Initialize the MQTT handler.
        :param broker: MQTT broker address.
        :param port: Port to connect to the broker.
        :param topics: List of topics to subscribe to.
        :param username: Username for MQTT authentication.
        :param password: Password for MQTT authentication.
        """
        self.broker = broker
        self.port = port
        self.topics = topics if topics else []  # Ensure topics is set
        self.username = username
        self.password = password

        self.client = mqtt.Client(client_id="", userdata=None)
        self.client.on_connect = self.on_connect  # Corrected to self.on_connect

        # Enable TLS for secure connection
        self.client.tls_set(tls_version=ssl.PROTOCOL_TLS)
        # Set username and password
        if self.username and self.password:
            self.client.username_pw_set(self.username, self.password)
        
        # Connect to HiveMQ Cloud
        self.client.connect(self.broker, self.port)

        # Setting callbacks
        self.client.on_subscribe = self.on_subscribe
        self.client.on_message = self.on_message
        self.client.on_publish = self.on_publish
        
        self.command_dispatcher = {
        "put": self.handle_put,
        # Add more commands here if necessary
        }

        # Start the loop in the background
        self.client.loop_start()
    
    def start(self):
        """Start the MQTT handler by subscribing to topics."""
        for topic in self.topics:
            self.client.subscribe(topic)
            print(f"Subscribed to topic: {topic}")
        print("MQTTHandler started and listening for messages.")

    def stop(self):
        """Stop the MQTT handler and disconnect from the broker."""
        self.client.loop_stop() 
        self.client.disconnect()
        print("MQTTHandler stopped and disconnected from the broker.")

    def on_connect(self, client, userdata, flags, rc):
        """Callback function for when the client connects to the broker."""
        if rc == 0:
            print("Connected successfully")
        else:
            print(f"Connection failed with code {rc}")

    def send_message(self, topic, message):
        """Publish a message to a specific topic."""
        self.client.publish(topic, message)

    def on_message(self, client, userdata, msg):
        """Callback for when a message is received."""
        message = msg.payload.decode('utf-8')
        print(f"Received message on topic {msg.topic}: {message}")
        self.handle_message(message)

    def on_publish(self, client, userdata, mid):
        """Callback for when a message is successfully published."""
        print(f"Message published with mid: {mid}")

    def on_subscribe(self, client, userdata, mid, granted_qos):
        """Callback for when a subscription is confirmed."""
        print(f"Subscribed to topic with mid: {mid} with QoS {granted_qos}")

    def handle_message(self, message):
        """Process the received message."""
        try:
            command, filename = self.parse_message(message)
            handler = self.command_dispatcher.get(command, self.handle_unknown)
            handler(filename if handler != self.handle_unknown else command)
        except ValueError as e:
            print(f"Error processing message: {e}")

    def parse_message(self, message):
        """Parse a message into command and filename."""
        try:
            command, filename = message.split("|", 1)  
            return command, filename
        except ValueError:
            raise ValueError("Invalid message format. Expected 'command|filename'.")

    def handle_put(self, filename):
        """Handle the 'put' command."""
        print(f"Processing PUT for {filename}")

    def handle_unknown(self, command):
        """Handle unknown commands."""
        print(f"Unknown command: {command}")
