import mysql.connector
from mysql.connector import Error
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MySQLStorage:
    def __init__(self, host, port, user, password, database):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connection = None
        self.connect()
    
    def connect(self):
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database
            )
            logger.info(f"Connected to MySQL database: {self.database}")
        except Error as e:
            logger.error(f"Error connecting to MySQL: {e}")
            self.connection = None
    
    def execute_query(self, query, params=None):
        try:
            cursor = self.connection.cursor(dictionary=True)
            cursor.execute(query, params or ())
            result = cursor.fetchall()
            cursor.close()
            return result
        except Error as e:
            logger.error(f"Query error: {e}")
            return []
    
    def execute_insert(self, query, values):
        try:
            cursor = self.connection.cursor()
            cursor.execute(query, values)
            self.connection.commit()
            affected = cursor.rowcount
            cursor.close()
            return affected
        except Error as e:
            logger.error(f"Insert error: {e}")
            self.connection.rollback()
            return 0
    
    def execute_update(self, query):
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            self.connection.commit()
            affected = cursor.rowcount
            cursor.close()
            return affected
        except Error as e:
            logger.error(f"Update error: {e}")
            self.connection.rollback()
            return 0
    
    def create_table(self, table_name, schema):
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({schema})"
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            self.connection.commit()
            cursor.close()
            logger.info(f"Table {table_name} created successfully")
            return True
        except Error as e:
            logger.error(f"Create table error: {e}")
            return False
    
    def store_chunk(self, chunk_id, data):
        query = """
            CREATE TABLE IF NOT EXISTS chunks (
                chunk_id VARCHAR(255) PRIMARY KEY,
                data LONGTEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        self.execute_update(query)
        
        insert_query = "INSERT INTO chunks (chunk_id, data) VALUES (%s, %s) ON DUPLICATE KEY UPDATE data = %s"
        return self.execute_insert(insert_query, (chunk_id, data, data))
    
    def close(self):
        if self.connection:
            self.connection.close()
            logger.info("MySQL connection closed")