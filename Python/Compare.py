import time
import os
import shutil

class Compare:
    def __init__(self, db_handler,mqtt_handler, dest_dir, model_dir,bestagentname, interval=10, threshold_percentage=5):
        """
        Initialize the Compare handler.
        :param db_handler: The database handler class.
        :param mqtt_handler: the mqtt handler class.
        :param interval: Time in seconds to wait between comparisons.
        :param threshold_percentage: the percentage the new model needs to be better then the old.
        :param dest_dir: The directory the models gets copied to.
        :param model_dir: the directory the models gets copied from.
        """
        super().__init__()
        self.db_handler = db_handler
        self.mqtt_handler = mqtt_handler
        self.interval = int(interval)
        self.running = True
        self.bestmodel = None
        self.threshold_percentage = float(threshold_percentage)
        self.dest_dir=dest_dir
        self.model_dir=model_dir
        self.Bestagentname=bestagentname


    def run(self):
        """Main loop for comparing models."""
        while self.running:
            try:
                self.find_best_model()
            except Exception as e:
                print(f"Error during comparison: {e}")
            time.sleep(self.interval)

    def find_best_model(self):
        """Fetch and identify the best model."""
        models = self.db_handler.get_newest_models()
        if len(models) < 2:
            print("Not enough models to compare.")
            return

        print("Determining the best model...")


        best_model = max(models, key=lambda m: m["model_score"])
        if best_model != self.bestmodel:
            if self.bestmodel is None or best_model["model_score"] >= self.bestmodel["model_score"] * (1 + self.threshold_percentage / 100):
                # Display the new best model
                print(f"Best Model: {best_model['username']} - {best_model}")
                try:
                    modelpath = os.path.join(self.model_dir, best_model['username'], best_model['filename'])
                    print(f"model from {modelpath}")
                    destPath = os.path.join(self.dest_dir,self.Bestagentname)
                    print(f"will be copied to {destPath}")
                    shutil.copy(modelpath,destPath)
                    print("file has been copied")
                    usernames = self.db_handler.get_all_users_except(best_model['username'])
                    for user in usernames:
                        payload=f"NewModel|{self.Bestagentname}"
                        topic=f"{user}/Commands"
                        self.mqtt_handler.send_message(topic,payload)
                    self.bestmodel = best_model
                except Exception as e:
                    print(f"Failed to process Model {e}")

            else:
                print(f"The best model's score is not {self.threshold_percentage}% higher than the current best. Current best model score: {self.bestmodel['model_score']}")
  
        else:
            print("It is the same best model as before.")
                   

    def stop(self):
        """Stop the comparison loop."""
        self.running = False
