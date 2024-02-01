import mysql.connector
import logging
import json


class MySQLConnector:
    port: int

    def __init__(self, host, username, password, database=None, port=3306, logger=None):
        """
        Initialize MySQLConnector with connection parameters.

        :param host: MySQL server host address
        :param port: MySQL server port
        :param username: MySQL username
        :param password: MySQL password
        :param database: Database name to connect
        :param logger: Logger object for logging (optional)
        """
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.logger = logger if logger else logging.getLogger(__name__)
        self.connection = None

    def connect(self):
        """
        Establish a connection to the MySQL database.

        :return: True if connection successful, False otherwise
        """
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.username,
                password=self.password,
                database=self.database,
                port=self.port
            )
            self.logger.info("Connected to MySQL database.")
            return True
        except mysql.connector.Error as err:
            self.logger.error(f"Error connecting to MySQL database: {err}")
            return False

    def disconnect(self):
        """
        Close the connection to the MySQL database.

        :return: True if disconnection successful, False otherwise
        """
        if self.connection:
            try:
                self.connection.close()
                self.logger.info("Disconnected from MySQL database.")
                return True
            except mysql.connector.Error as err:
                self.logger.error(f"Error disconnecting from MySQL database: {err}")
                return False
        else:
            self.logger.warning("No active connection to disconnect.")
            return True

    def execute_query(self, query, data=None):
        """
        Execute a query on the MySQL database.

        :param query: SQL query to execute
        :param data: Data to be used in the query (optional)
        :return: Result of the query or None if an error occurs
        """
        try:
            cursor = self.connection.cursor(dictionary=True)  # Use dictionary cursor to get results as dictionaries
            cursor.execute(query, data)
            result = cursor.fetchall()
            self.connection.commit()
            cursor.close()
            self.logger.info(f"Query executed successfully: {query}")
            return result
        except mysql.connector.Error as err:
            self.logger.error(f"Error executing query: {err}")
            return None


class MySQLOperations:
    def __init__(self, connector):
        self.connector = connector
        self.logger = self.connector.logger

    def create_database(self, database_name, replace_if_exists=False):
        try:
            if replace_if_exists:
                self.drop_database(database_name)
            query = f"CREATE DATABASE IF NOT EXISTS {database_name};"
            self.connector.execute_query(query)
            self.logger.info(f"Database '{database_name}' created successfully.")
            return True
        except mysql.connector.Error as err:
            self.logger.error(f"Error creating database '{database_name}': {err}")
            return False

    def create_table(self, table_name, columns_definition, replace_if_exists=False):
        try:
            if replace_if_exists:
                self.drop_table(table_name)
            query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_definition});"
            self.connector.execute_query(query)
            self.logger.info(f"Table '{table_name}' created successfully.")
            return True
        except mysql.connector.Error as err:
            self.logger.error(f"Error creating table '{table_name}': {err}")
            return False

    def insert_data(self, table_name, data):
        try:
            columns = ', '.join(data.keys())
            values = ', '.join(['%s'] * len(data))
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({values});"
            self.connector.execute_query(query, tuple(data.values()))
            self.logger.info(f"Data inserted into '{table_name}' successfully.")
            return True
        except mysql.connector.Error as err:
            self.logger.error(f"Error inserting data into '{table_name}': {err}")
            return False

    def select_data(self, table_name, conditions=None, order_by=None, limit=None):
        try:
            condition_str = f" WHERE {conditions}" if conditions else ""
            order_by_str = f" ORDER BY {order_by}" if order_by else ""
            limit_str = f" LIMIT {limit}" if limit else ""
            query = f"SELECT * FROM {table_name}{condition_str}{order_by_str}{limit_str};"
            result = self.connector.execute_query(query)
            self.logger.info(f"Data selected from '{table_name}' successfully.")
            return result
        except mysql.connector.Error as err:
            self.logger.error(f"Error selecting data from '{table_name}': {err}")
            return None

    def update_data(self, table_name, data, conditions):
        try:
            set_clause = ', '.join([f"{col} = %s" for col in data.keys()])
            other_conditions = ' AND '.join([f"{condition}" for condition in conditions]) if conditions else None
            condition_str = f" WHERE {other_conditions}" if other_conditions else ""
            query = f"UPDATE {table_name} SET {set_clause}{condition_str};"
            self.logger.info(f"Query updated: '{query}'")
            self.connector.execute_query(query, tuple(data.values()))
            self.logger.info(f"Data updated in '{table_name}' successfully.")
            return True
        except mysql.connector.Error as err:
            self.logger.error(f"Error updating data in '{table_name}': {err}")
            return False

    def delete_data(self, table_name, conditions):
        try:
            condition_str = f" WHERE {conditions}"
            query = f"DELETE FROM {table_name}{condition_str};"
            self.connector.execute_query(query)
            self.logger.info(f"Data deleted from '{table_name}' successfully.")
            return True
        except mysql.connector.Error as err:
            self.logger.error(f"Error deleting data from '{table_name}': {err}")
            return False

    def drop_database(self, database_name):
        try:
            query = f"DROP DATABASE IF EXISTS {database_name};"
            self.connector.execute_query(query)
            self.logger.info(f"Database '{database_name}' dropped successfully.")
            return True
        except mysql.connector.Error as err:
            self.logger.error(f"Error dropping database '{database_name}': {err}")
            return False

    def drop_table(self, table_name):
        try:
            query = f"DROP TABLE IF EXISTS {table_name};"
            self.connector.execute_query(query)
            self.logger.info(f"Table '{table_name}' dropped successfully.")
            return True
        except mysql.connector.Error as err:
            self.logger.error(f"Error dropping table '{table_name}': {err}")
            return False

    def drop_index(self, index_name, table_name):
        try:
            query = f"DROP INDEX IF EXISTS {index_name} ON {table_name};"
            self.connector.execute_query(query)
            self.logger.info(f"Index '{index_name}' dropped successfully from '{table_name}'.")
            return True
        except mysql.connector.Error as err:
            self.logger.error(f"Error dropping index '{index_name}' from '{table_name}': {err}")
            return False

    def drop_column(self, column_name, table_name):
        try:
            query = f"ALTER TABLE {table_name} DROP COLUMN {column_name};"
            self.connector.execute_query(query)
            self.logger.info(f"Column '{column_name}' dropped successfully from '{table_name}'.")
            return True
        except mysql.connector.Error as err:
            self.logger.error(f"Error dropping column '{column_name}' from '{table_name}': {err}")
            return False

    def truncate_database(self, database_name):
        try:
            query = f"TRUNCATE {database_name};"
            self.connector.execute_query(query)
            self.logger.info(f"Database '{database_name}' truncated successfully.")
            return True
        except mysql.connector.Error as err:
            self.logger.error(f"Error truncating database '{database_name}': {err}")
            return False

    def truncate_table(self, table_name):
        try:
            query = f"TRUNCATE TABLE {table_name};"
            self.connector.execute_query(query)
            self.logger.info(f"Table '{table_name}' truncated successfully.")
            return True
        except mysql.connector.Error as err:
            self.logger.error(f"Error truncating table '{table_name}': {err}")
            return False

    def truncate_index(self, index_name, table_name):
        try:
            query = f"TRUNCATE INDEX {index_name} ON {table_name};"
            self.connector.execute_query(query)
            self.logger.info(f"Index '{index_name}' truncated successfully on '{table_name}'.")
            return True
        except mysql.connector.Error as err:
            self.logger.error(f"Error truncating index '{index_name}' on '{table_name}': {err}")
            return False

    def truncate_column(self, column_name, table_name):
        try:
            query = f"TRUNCATE COLUMN {column_name} ON {table_name};"
            self.connector.execute_query(query)
            self.logger.info(f"Column '{column_name}' truncated successfully on '{table_name}'.")
            return True
        except mysql.connector.Error as err:
            self.logger.error(f"Error truncating column '{column_name}' on '{table_name}': {err}")
            return False
