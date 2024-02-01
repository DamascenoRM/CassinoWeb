import asyncio
import logging

from lib.Database import MySQLConnector, MySQLOperations

async def main():
    # Setup logger
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # Database connection parameters
    host = "localhost"
    username = "root"
    password = "root"
    database_name = "test_db"

    # Create MySQLConnector instance
    db_connector = MySQLConnector(host=host, username=username, password=password, database=database_name, logger=logger)

    # Connect to the database
    if not db_connector.connect():
        return

    # Create MySQLOperations instance
    db_operations = MySQLOperations(db_connector)

    try:
        # Create test database
        if not db_operations.create_database(database_name):
            return

        # Create tables
        columns_definition = "id INT AUTO_INCREMENT PRIMARY KEY, texto VARCHAR(255)"
        for table_name in ["tabela1", "tabela2", "tabela3"]:
            if not db_operations.create_table(table_name, columns_definition):
                return

        # Insert data into tables
        data_to_insert = {"texto": "ok"}
        for table_name in ["tabela1", "tabela2", "tabela3"]:
            for _ in range(3):
                db_operations.insert_data(table_name, data_to_insert)

        # Select and display data
        for table_name in ["tabela1", "tabela2", "tabela3"]:
            result = db_operations.select_data(table_name)
            print(f"Data in '{table_name}': {result}")

        # Pause and wait for user input
        input("Update ?...")

        # Update data in tabela1
        update_data = {"texto": "atualizado"}
        conditions = ["3=3", "2=2"]
        #conditions = None
        db_operations.update_data(table_name="tabela1", data=update_data, conditions=conditions)

        # Pause and wait for user input
        input("Press Enter to continue...")

        # Delete all rows from all tables
        for table_name in ["tabela1", "tabela2", "tabela3"]:
            db_operations.delete_data(table_name, "1=1")

    finally:
        # Disconnect from the database
        db_connector.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
