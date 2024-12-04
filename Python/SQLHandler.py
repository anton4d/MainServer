import mysql.connector
from mysql.connector import errorcode

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
            print("Connected to the database successfully.")
            self.setup_schema()
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Invalid username or password.")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print(f"Database '{self.database}' does not exist.")
            else:
                print(err)
    
    def setup_schema(self):
        """Create the necessary tables if they do not exist."""
        try:
            create_table_query = """
            CREATE TABLE IF NOT EXISTS files (
                id INT AUTO_INCREMENT PRIMARY KEY,
                filename VARCHAR(255) NOT NULL,
                uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            self.cursor.execute(create_table_query)
            self.connection.commit()
            print("Database schema is set up.")
        except mysql.connector.Error as err:
            print(f"Error creating schema: {err}")

    def put(self, filename):
        """Insert a new file record into the database."""
        try:
            insert_query = "INSERT INTO files (filename) VALUES (%s)"
            self.cursor.execute(insert_query, (filename,))
            self.connection.commit()
            print(f"Inserted new file '{filename}' into the database.")
        except mysql.connector.Error as err:
            print(f"Error inserting data: {err}")

    def get(self, filename):
        """Retrieve a file record from the database."""
        try:
            select_query = "SELECT * FROM files WHERE filename = %s"
            self.cursor.execute(select_query, (filename,))
            result = self.cursor.fetchall()
            if result:
                print(f"Found file: {result}")
                return result
            else:
                print(f"File '{filename}' not found in the database.")
                return None
        except mysql.connector.Error as err:
            print(f"Error fetching data: {err}")
            return None

    def close(self):
        """Close the database connection."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        print("Connection closed.")

# Example Usage
if __name__ == "__main__":
    # Database connection parameters
    db_host = 'localhost'
    db_user = 'root'
    db_password = 'root_password'  # Use your password here
    db_name = 'test_db'  # Database to be created

    # Create SQLHandler instance
    sql_handler = SQLHandler(host=db_host, user=db_user, password=db_password, database=db_name)

    # Put a new file record in the database
    sql_handler.put("test_file.txt")

    # Get the file record from the database
    sql_handler.get("test_file.txt")

    # Close the connection when done
    sql_handler.close()
