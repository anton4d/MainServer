import time
import dotenv
import os
import logging

class Test:
    def __init__(self, mqtt_handler, stop_event):
        self.mqtthandler = mqtt_handler
        self.stop_event = stop_event
    
    def loop(self):
        timeoftest = 5*60*60
        
        time.sleep(10)
        logging.info("Stopping all ongoing training")
        dotenv_file = dotenv.find_dotenv()
        dotenv.load_dotenv(dotenv_file)
        
        testseed = os.getenv("TestSED")
        seed = os.getenv("SEED")
        maxit = os.getenv("MaxIterations")
        test_max_it = os.getenv("TestMaxIterations")
        filename = os.getenv("BestFilePath")
        
        topic = "all/Commands"
        payload = "StopTrain|"
        self.mqtthandler.send_message(topic, payload)
        
        topic = "all/Commands"
        payload = f"Setup|{maxit}|{seed}|{test_max_it}|{testseed}|{filename}"
        logging.info("Starting all training")
        self.mqtthandler.send_message(topic, payload)
        
        time.sleep(timeoftest)
        
        topic = "all/Commands"
        payload = "StopTrain|"
        self.mqtthandler.send_message(topic, payload)
        logging.info("Test is done")
        
        self.stop_event.set()
