import time
import os
import shutil
import logging

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
                logging(f"Error during comparison: {e}")
            time.sleep(self.interval)

    def find_best_model(self):
        """Fetch and identify the best model."""
        models = self.db_handler.get_newest_models()
        if len(models) == 0:
            logging.info("No Models to compare or use")
            return
        if len(models) < 2:
            logging.info("Not enough models to compare. so just uses newest model")
            Newestmodel = models[0]
            modelpath = os.path.join(self.model_dir, Newestmodel['username'], Newestmodel['filename'])
            logging.info(f"model from {modelpath}")
            destPath = os.path.join(self.dest_dir,self.Bestagentname)
            logging.info(f"will be copied to {destPath}")
            if not os.path.isfile(modelpath):
                time.sleep(20)
            if os.path.isfile(modelpath):
                shutil.copy(modelpath, destPath)
                logging.info("file has been copied")
                return
            else:
                logging.info("File is still not available after waiting.")
                return
            
            

        logging.info("Determining the best model...")


        best_model = max(models, key=lambda m: m["model_score"])
        if best_model != self.bestmodel:
            if self.bestmodel is None or best_model["model_score"] >= self.bestmodel["model_score"] * (1 + self.threshold_percentage / 100):
                # Display the new best model
                logging.info(f"Best Model: {best_model['username']} - {best_model}")
                try:
                    modelpath = os.path.join(self.model_dir, best_model['username'], best_model['filename'])
                    logging.info(f"model from {modelpath}")
                    destPath = os.path.join(self.dest_dir,self.Bestagentname)
                    logging.info(f"will be copied to {destPath}")
                    if not os.path.isfile(modelpath):
                        time.sleep(20)
                    if os.path.isfile(modelpath):
                        shutil.copy(modelpath,destPath)
                        logging.info("file has been copied")
                        usernames = self.db_handler.get_all_users_except(best_model['username'])
                        for user in usernames:
                            payload=f"NewModel|{self.Bestagentname}"
                            topic=f"{user}/Commands"
                            self.mqtt_handler.send_message(topic,payload)
                        self.bestmodel = best_model
                        return
                    else:
                        logging.error("File is still not available after waiting.")
                        return
                    
                except Exception as e:
                    logging.error(f"Failed to process Model {e}")

            else:
                logging.info(f"The best model's score is not {self.threshold_percentage}% higher than the current best. Current best model score: {self.bestmodel['model_score']}")
  
        else:
            logging.info("It is the same best model as before.")
                   

    def stop(self):
        """Stop the comparison loop."""
        self.running = False
