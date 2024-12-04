from FtpMonitor import FtpHandler
from MqttHandler import MQTTHandler
from SQLHandler import SQLHandler
from HDFSHandler import HDFSHandler 

def main():
    WATCH_DIR = "/srv/sftp/sftpuser/uploads"
    DEST_DIR = "/srv/EIT/MainServer/test"

    # Setup MQTTHandler with the correct credentials
    mqtt_handler = MQTTHandler(
        broker="7916c2f7506a48358338a1ddb98af564.s1.eu.hivemq.cloud",
        port=8883,
        topics=["file/notifications"], 
        username="hivemq.webclient.1733261441806",
        password="C!>D<A*B3012FbHdqacf" 
    )

    # Setup SQLHandler with database details
    db_handler = SQLHandler(
        host="localhost",
        user="test_user", 
        password="test_password",
        database="test_db"  
    )

    # Setup HDFSHandler with HDFS connection details
    hdfs_handler = HDFSHandler(
        hdfs_url="http://Localhost:50070", 
        user="hdfs" 
    )

    # Setup FtpHandler with watch and destination directories
    ftp_handler = FtpHandler(
        watch_dir=WATCH_DIR,
        dest_dir=DEST_DIR,
        mqtt_handler=mqtt_handler,
        topic="file/notifications",
        db_handler=db_handler 
    )

    try:
        # Start the FTP, MQTT handlers, SQL handler, and HDFS handler
        ftp_handler.start()
        mqtt_handler.start()

        # Keep running the script until Enter is pressed
        input("Press Enter to stop...\n")
    finally:
        # Stop the FTP handler gracefully
        ftp_handler.stop()
        
        # Stop the MQTT handler gracefully
        mqtt_handler.stop()

        # Ensure to close the SQL database connection when the program ends
        db_handler.close()

        # Stop the HDFS handler
        hdfs_handler.stop()  

        print("Stopped all services.")

if __name__ == "__main__":
    main()
