import paho.mqtt.client as mqtt
import ssl
import dotenv
import os

class MQTTHandler:
    def __init__(self, broker, port, topics=None, username=None, password=None, db_handler=None):
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
        self.topics = topics if topics else []
        self.username = username
        self.password = password
        self.db_handler=db_handler

        self.client = mqtt.Client(client_id="", userdata=None)
        self.client.on_connect = self.on_connect  

        self.client.tls_set(tls_version=ssl.PROTOCOL_TLS)
        if self.username and self.password:
            self.client.username_pw_set(self.username, self.password)
        
        self.client.connect(self.broker, self.port)

        self.client.on_subscribe = self.on_subscribe
        self.client.on_message = self.on_message
        self.client.on_publish = self.on_publish
        
        self.command_dispatcher = {
        "TestResultat": self.handle_testResult,
        "LoopStarted": self.handle_started,
        "LoopStoped": self.handle_stopped,
        "NewUser": self.handle_NewUser,
        # Add more commands here if necessary
        }

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
        topic = msg.topic
        print(topic)
        username = self.extract_username(topic)
        print(f"Received message on topic {topic}: {message}")
        self.handle_message(message, username)

    def on_publish(self, client, userdata, mid):
        """Callback for when a message is successfully published."""
        print(f"Message published with mid: {mid}")

    def on_subscribe(self, client, userdata, mid, granted_qos):
        """Callback for when a subscription is confirmed."""
        print(f"Subscribed to topic with mid: {mid} with QoS {granted_qos}")

    def extract_username(self, topic):
        """
        Extract the username from the MQTT topic.
        Assumes the topic structure starts with 'Server/username/...'.

        :param topic: The MQTT topic string.
        :return: The extracted username or None if the structure is invalid.
        """
        try:
            # Split the topic by '/' and validate the structure
            parts = topic.split('/')
            if len(parts) >= 2 and parts[0] == 'Server':
                return parts[1]  # The username is the second part
            else:
                print("Invalid topic structure.")
                return None
        except Exception as e:
            print(f"Error parsing topic: {e}")
            return None

    def handle_message(self, message, username):
        """Process the received message."""
        try:
            command, data = self.parse_message(message)
            handler = self.command_dispatcher.get(command, self.handle_unknown)
            if handler != self.handle_unknown:
                handler(data,username)
            else:
                handler(command, username)
        except ValueError as e:
            print(f"Error processing message: {e}")

    def parse_message(self, message):
        """Parse a message into command and data."""
        try:
            command, data = message.split("|", 1)  
            return command, data
        except ValueError:
            raise ValueError("Invalid message format. Expected 'command|data...'.")


    def handle_testResult(self, data, username):
        """Handle the 'TestResult' command and compute ModelScore."""
        print(f"Processing test for user {username}, with data {data}")
        try:
            dotenvFile = dotenv.find_dotenv()
            dotenv.load_dotenv(dotenvFile, override=True)
            mean_weight = float(os.getenv("meanWeight"))
            std_weight = float(os.getenv("stdWeight"))

            FileName, rewardMean, rewardStd = data.split("|", 2)
        
            # Convert the values
            reward_mean = float(rewardMean)
            reward_std = float(rewardStd)

            # Calculate ModelScore
            model_score = self.calculate_model_score(reward_mean, reward_std, mean_weight, std_weight)
        
            # Insert data into the database
            self.db_handler.InsertModel(FileName, reward_mean, reward_std, username ,model_score)
            print(f"Inserted model with score {model_score} for user {username}.")
        except ValueError:
            raise ValueError("Invalid message format. Expected 'data|data|data..'.")
        except Exception as e:
            print(f"An error occurred: {e}")

    def calculate_model_score(self, reward_mean, reward_std, mean_weight=1.0, std_weight=1.0):
        """
        Calculate the ModelScore based on reward mean, reward standard deviation, 
        and their respective weights.

        Parameters:
            reward_mean (float): The mean reward of the model.
            reward_std (float): The standard deviation of the rewards.
            mean_weight (float): Weight multiplier for the reward mean.
            std_weight (float): Weight multiplier for the reward standard deviation.

        Returns:
            float: The calculated ModelScore.
        """
        model_score = (reward_mean * mean_weight) - (reward_std * std_weight)
        return model_score


    
    def handle_started(self, data, username):
        """Handle the 'started training' command."""
        print(f"Processing started for user {username}, with data {data}")

    def handle_stopped(self, data, username):
        """Handle the 'Stopped training' command."""
        print(f"Processing stopped for user {username}, with data {data}")

    def handle_NewUser(self, data, username):
        """Handle the 'NewUser' command."""
        print(f"processing newUser with username {data}")
        self.db_handler.InsertUser(data)
        print(f"inserted user with username {data}")
        dotenvFile = dotenv.find_dotenv()
        dotenv.load_dotenv(dotenvFile, override=True)
        seed= os.getenv("SEED")
        testseed= os.getenv("TestSED")
        maxietarions= os.getenv("MaxIterations")
        testiterations= os.getenv("TestMaxIterations")
        filepath= os.getenv("BestFilePath")
        payload=f"Setup|{maxietarions}|{seed}|{testiterations}|{testseed}|{filepath}"
        topic=f"{data}/Commands"
        self.send_message(topic, payload)

    def handle_unknown(self, command, username):
        """Handle unknown commands."""
        print(f"Unknown command: {command}, from user {username}")
