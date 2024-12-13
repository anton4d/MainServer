import mysql.connector
from mysql.connector import errorcode
import logging

class SQLHandler:
    def __init__(self, host, user, password, database):
        """
        Initialize the SQL handler.
        :param host: Database host (e.g., 'localhost' or a docker container IP).
        :param user: Database user (e.g., 'root').
        :param password: Database password.
        :param database: Database name.
        """
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None
        self.cursor = None

        self.connect()

    def connect(self):
        """Connect to the MySQL database."""
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            self.cursor = self.connection.cursor()
            logging.info("Connected to the database successfully.")
            self.setup_schema()
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                logging.info("Invalid username or password.")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                logging.info(f"Database '{self.database}' does not exist.")
            else:
                logging.error(err)

    def setup_schema(self):
        """Create the necessary tables if they do not exist."""
        try:
            create_table_query = """
            CREATE TABLE IF NOT EXISTS Users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                Username VARCHAR(255) NOT NULL,
                CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            self.cursor.execute(create_table_query)
            self.connection.commit()

            create_table_query = """
            CREATE TABLE IF NOT EXISTS Models (
                id INT AUTO_INCREMENT PRIMARY KEY,
                FileName VARCHAR(255) NOT NULL,
                rewardMean DOUBLE NOT NULL,
                rewardStd DOUBLE NOT NULL,
                ModelScore DOUBLE NOT NULL,
                USERID INT NOT NULL,
                uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (USERID) REFERENCES Users(id)
            );
            """
            self.cursor.execute(create_table_query)
            self.connection.commit()

            logging.info("Database schema is set up.")
        except mysql.connector.Error as err:
            logging.error(f"Error creating schema: {err}")

    def InsertModel(self, filename, rewardMean, rewardStd, username, modelScore):
        """Insert a new Model record into the database using the username to find the USERID."""
        try:
            # Find the USERID based on the username
            find_user_query = "SELECT id FROM Users WHERE Username = %s"
            self.cursor.execute(find_user_query, (username,))
            result = self.cursor.fetchone()
            
            if result is None:
                logging.info(f"Error: No user found with username '{username}'.")
                return
            
            userid = result[0]  # Extract USERID from the query result
            
            # Insert the new file record into the Models table
            insert_query = """
            INSERT INTO Models (FileName, rewardMean, rewardStd, USERID, ModelScore) 
            VALUES (%s, %s, %s, %s, %s)
            """
            self.cursor.execute(insert_query, (filename, rewardMean, rewardStd, userid, modelScore,))
            self.connection.commit()
            logging.info(f"Inserted new file '{filename}' into the database for user '{username}'.")
        except mysql.connector.Error as err:
            logging.error(f"Error inserting data: {err}")
            raise

    def getModel(self, filename):
        """Retrieve a Model record from the database."""
        try:
            select_query = "SELECT * FROM Models WHERE FileName = %s"
            self.cursor.execute(select_query, (filename,))
            result = self.cursor.fetchall()
            if result:
                logging.info(f"Found Model: {result}")
                return result
            else:
                logging.info(f"Model '{filename}' not found in the database.")
                return None
        except mysql.connector.Error as err:
            logging.error(f"Error fetching data: {err}")
            return None
        
    def get_newest_models(self):
        """
        Retrieve the newest model for each user, along with metadata.
        The result will be a list in the format: [username:model_with_metadata, ...].
        """
        try:
            # Query to get the newest model for each user
            query = """
            SELECT 
                u.Username,
                m.FileName,
                m.rewardMean,
                m.rewardStd,
                m.ModelScore,
                m.uploaded_at
            FROM 
                Users u
            JOIN 
                Models m ON u.id = m.USERID
            WHERE 
                m.uploaded_at = (
                    SELECT MAX(m2.uploaded_at) 
                    FROM Models m2 
                    WHERE m2.USERID = m.USERID
                )
            ORDER BY 
                u.Username;
            """

            self.cursor.execute(query)
            results = self.cursor.fetchall()

            newest_models = [
                {
                    "username": row[0],
                    "filename": row[1],
                    "reward_mean": row[2],
                    "reward_std": row[3],
                    "model_score": row[4],
                    "uploaded_at": row[5],
                }
                for row in results
            ]   
            logging.info("got all the newest models ")
            return newest_models
        except mysql.connector.Error as err:
            logging.error(f"Error fetching newest models: {err}")
            return []
        
    def get_all_users_except(self, excluded_username):
        """
        Retrieve all usernames except for the one specified (e.g., for messaging purposes).
        :param excluded_username: The username to exclude from the result.
        :return: A list of usernames excluding the specified username.
        """
        try:
            # Query to get all usernames except the excluded one
            query = """
            SELECT Username
            FROM Users
            WHERE Username != %s
            """

            self.cursor.execute(query, (excluded_username,))
            results = self.cursor.fetchall()

            # Extract usernames from the result set
            usernames = [row[0] for row in results]

            logging.info(f"Retrieved all users except {excluded_username}: {usernames}")
            return usernames
        except mysql.connector.Error as err:
            logging.error(f"Error fetching users excluding {excluded_username}: {err}")
            return []


    def InsertUser(self, username):
        """Insert a new username into the database"""
        try:
            find_user_query = "SELECT id FROM Users WHERE Username = %s"
            self.cursor.execute(find_user_query, (username,))
            result = self.cursor.fetchone()
            if result is None:
                logging.info(f"No user found, will insert user with username: '{username}'.")
                insert_query = """
                INSERT INTO Users (Username) 
                VALUES (%s)
                """
                self.cursor.execute(insert_query, (username,))
                self.connection.commit()
            else:
                logging.info(f"Error: User is already in database with username: {username}.")
        except mysql.connector.Error as err:
            logging.error(f"Error fetching data: {err}")
            return None
        
    def getUser(self,username):
        try:
            find_user_query = "SELECT id FROM Users WHERE Username = %s"
            self.cursor.execute(find_user_query, (username,))
            result = self.cursor.fetchone()
            if result:
                logging.info(f"Found user: {username}")
                return result
            else:
                logging.info(f"user '{username}' not found in the database.")
                return None
        except mysql.connector.Error as err:
            logging.error(f"Error fetching data: {err}")
            return None
    def close(self):
        """Close the database connection."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logging.info("Connection closed.")

if __name__ == "__main__":
    
    db_host = 'localhost'
    db_user = 'root'
    db_password = 'root_password'  
    db_name = 'test_db'  

    
    sql_handler = SQLHandler(host=db_host, user=db_user, password=db_password, database=db_name)

    sql_handler.close()
