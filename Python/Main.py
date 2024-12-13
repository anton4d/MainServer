import threading
from FtpMonitor import FtpHandler
from MqttHandler import MQTTHandler
from SQLHandler import SQLHandler
from Compare import Compare
from Test import Test
import dotenv
import os
import time
import logging

def setup_logging(log_file="app.log"):
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,  # Set the desired logging.info level
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

def main():

    dotenvFile = dotenv.find_dotenv()
    dotenv.load_dotenv(dotenvFile)
    WATCH_DIR = os.getenv("FTPHANDLERWATCHDIR")
    DEST_DIR = os.getenv("FTPHANDLERDESTDIR")
    Download_dir = os.getenv("FTPDownloadDirPath")
    BestModelName = os.getenv("BestFilePath")

    db_handler = SQLHandler(
        host=os.getenv("SQLHOST"),
        user=os.getenv("DBUSER"),
        password=os.getenv("DBPASSWORD"),
        database=os.getenv("DBDB")
    )

    mqtt_handler = MQTTHandler(
        broker=os.getenv("MQTTBROKER"),
        port=int(os.getenv("MQTTPORT")),
        topics=[os.getenv("MQTTTOPICS")],
        username=os.getenv("MQTTUSERNAME"),
        password=os.getenv("MQTTPASSWORD"),
        db_handler=db_handler
    )

    ftp_handler = FtpHandler(
        watch_dir=WATCH_DIR,
        dest_dir=DEST_DIR,
        mqtt_handler=mqtt_handler,
        topic=os.getenv("FTPHANDLERTOPIC")
    )
    
    interval = os.getenv("Interval")
    threshold_percentage = os.getenv("thresholdPercentage")
    compare_handler = Compare(
        db_handler=db_handler,
        mqtt_handler=mqtt_handler,
        interval=interval,
        threshold_percentage=threshold_percentage,
        dest_dir=Download_dir,
        model_dir=DEST_DIR,
        bestagentname=BestModelName
    )
    
    # Create a threading event for graceful shutdown
    stop_event = threading.Event()
    
    # Pass stop_event to Test
    test = Test(mqtt_handler=mqtt_handler, stop_event=stop_event)

    def run_compare():
        logging.info("Starting Compare thread...")
        compare_handler.run()

    def run_test():
        logging.info("Starting Test thread...")
        test.loop()

    try:
        # Start services
        ftp_handler.start()

        compare_thread = threading.Thread(target=run_compare, daemon=True)
        compare_thread.start()


        test_thread = threading.Thread(target=run_test, daemon=True)
        test_thread.start()



        logging.info("Press Enter to stop, or wait for the test to finish...")

        while not stop_event.is_set():
            time.sleep(1)

        logging.info("Exiting program...")

    finally:

        ftp_handler.stop()
        mqtt_handler.stop()
        db_handler.close()
        compare_handler.stop()

        logging.info("Stopped all services.")

if __name__ == "__main__":
    setup_logging() 
    logging.info("Starting the application...")
    main()
