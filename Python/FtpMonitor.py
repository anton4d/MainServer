import os
import shutil
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class FtpHandler:
    def __init__(self, watch_dir, dest_dir, mqtt_handler=None, topic=None):
        """
        Initialize the FTP handler.
        :param watch_dir: Directory to monitor.
        :param dest_dir: Directory to copy files to.
        :param mqtt_handler: Instance of MQTTHandler to send messages.
        :param topic: MQTT topic to publish messages to.
        """
        self.watch_dir = watch_dir
        self.dest_dir = dest_dir
        self.mqtt_handler = mqtt_handler
        self.topic = topic
        self.observer = Observer()

    def start(self):
        """Start monitoring the directory."""
        event_handler = UploadEventHandler(self.dest_dir, self.mqtt_handler, self.topic)
        self.observer.schedule(event_handler, self.watch_dir, recursive=False)
        self.observer.start()
        print(f"FtpHandler started. Watching directory: {self.watch_dir}")

    def stop(self):
        """Stop monitoring the directory."""
        self.observer.stop()
        self.observer.join()
        print("FtpHandler stopped.")


class UploadEventHandler(FileSystemEventHandler):
    def __init__(self, dest_dir, mqtt_handler=None, topic=None):
        """
        Initialize the event handler.
        :param dest_dir: Directory to copy files to.
        :param mqtt_handler: Instance of MQTTHandler to send messages.
        :param topic: MQTT topic to publish messages to.
        """
        self.dest_dir = dest_dir
        self.mqtt_handler = mqtt_handler
        self.topic = topic

    def on_created(self, event):
        """Handle file creation events."""
        if not event.is_directory:
            src_path = event.src_path
            file_name = os.path.basename(src_path)

            try:

                username = file_name.split('_')[0]

                user_dir = os.path.join(self.dest_dir, username)
                if not os.path.exists(user_dir):
                    os.makedirs(user_dir)
                    print(f"Directory created: {user_dir}")
                time.sleep(20)
                dest_path = os.path.join(user_dir, file_name)
                shutil.copy(src_path, dest_path)
                print(f"File copied from {src_path} to {dest_path}")

                os.remove(src_path)
                print(f"File deleted from source: {src_path}")
        
            except Exception as e:
                print(f"Failed to process file {src_path}: {e}")
