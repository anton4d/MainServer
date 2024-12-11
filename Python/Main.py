from FtpMonitor import FtpHandler
from MqttHandler import MQTTHandler
from SQLHandler import SQLHandler
from Compare import Compare
import dotenv
import os

def main():
    dotenvFile = dotenv.find_dotenv()
    dotenv.load_dotenv(dotenvFile)
    WATCH_DIR = os.getenv("FTPHANDLERWATCHDIR")
    DEST_DIR = os.getenv("FTPHANDLERDESTDIR")
    Download_dir = os.getenv("FTPDownloadDirPath")
    BestModelName = os.getenv("BestFilePath")


    db_handler = SQLHandler(
        host= os.getenv("SQLHOST"),
        user= os.getenv("DBUSER"), 
        password= os.getenv("DBPASSWORD"),
        database= os.getenv("DBDB")
    )

   
    mqtt_handler = MQTTHandler(
        broker= os.getenv("MQTTBROKER"),
        port= int(os.getenv("MQTTPORT")),
        topics=[os.getenv("MQTTTOPICS")], 
        username= os.getenv("MQTTUSERNAME"),
        password= os.getenv("MQTTPASSWORD"),
        db_handler=db_handler
    )

    ftp_handler = FtpHandler(
        watch_dir=WATCH_DIR,
        dest_dir=DEST_DIR,
        mqtt_handler=mqtt_handler,
        topic= os.getenv("FTPHANDLERTOPIC")
    )
    interval= os.getenv("Interval")
    threshold_percentage= os.getenv("thresholdPercentage")
    compare_handler = Compare(
        db_handler=db_handler, 
        mqtt_handler=mqtt_handler ,
        interval=interval,
        threshold_percentage=threshold_percentage,
        dest_dir=Download_dir,
        model_dir=DEST_DIR,
        bestagentname=BestModelName
    )

    try:
        ftp_handler.start()
        mqtt_handler.start()
        compare_handler.run()

        input("Press Enter to stop...\n")
    finally:

        ftp_handler.stop()
        
        mqtt_handler.stop()

        db_handler.close()
        compare_handler.stop()

        print("Stopped all services.")

if __name__ == "__main__":
    main()
