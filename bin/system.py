import logging
import time
import os
from dotenv import load_dotenv
from colorama import Fore, Style
import datetime

from lib.Database import MySQLConnector, MySQLOperations


def get_env_config(env_file=".env", logger=logging.getLogger(__name__)):
    # Load .env file if exist
    if os.path.exists(env_file):
        logger.info("Env file successful load.")
        load_dotenv(env_file)
        logger.setLevel(int(os.environ.get("LOG_LEVEL")) if os.environ.get("LOG_LEVEL") else logger.level)
        print(
            f"{Fore.RED}{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]} {Fore.BLUE}- {Fore.GREEN}Logging level set to {logging.getLevelName(logger.level)}{Style.RESET_ALL}")
        try:
            with open(env_file, "r") as file:
                for line in file:
                    logger.debug(f"Read from .env file: {line.strip()}")
        except Exception as e:
            logger.error(f"Error in {env_file}: {str(e)}")
        for var in os.environ:
            logger.debug(f"{var}={os.environ.get(var)}")


    else:
        logger.info("Env file not found.")


async def maintainer(logger=logging.getLogger(__name__), update_time=60):

    while True:
        logger.info("Update system config: started.")
        try:
            db_connector = MySQLConnector(host=os.environ.get("DB_HOST"), username=os.environ.get("DB_USER"),
                                          password=os.environ.get("DB_PASSWORD"), database=os.environ.get("DB_NAME"),
                                          port=int(os.environ.get("DB_PORT")), logger=logger)
            # Connect to the database
            if not db_connector.connect():
                return

            # Create MySQLOperations instance
            db_operations = MySQLOperations(db_connector)
            try:
                results = db_operations.select_data("parameters")
                for result in results:
                    os.environ[result['pkey']] = str(result['value'])
                    logger.debug(f"Load {result['pkey']} is {str(result['value'])}")
            finally:
                # Disconnect from the database
                db_connector.disconnect()
        except Exception as e:
            logger.error(f"Error: {str(e)}")

        try:
            logger.setLevel(int(os.environ.get("LOG_LEVEL")) if os.environ.get("LOG_LEVEL") else logger.level)
            logger.debug(f"Logging level set to {logging.getLevelName(logger.level)}")
        except Exception as e:
            logger.error(
                f"Env 'LOG_LEVEL' is invalid '{os.environ.get('LOG_LEVEL')}', set a integer number CRITICAL = 50 | "
                f"ERROR = 40 | WARNING = 30 | INFO = 20 | DEBUG = 10 | NOTSET = 0")

        try:
            update_time = int(os.environ.get('RELOAD_CONF')) if os.environ.get('RELOAD_CONF') else update_time
            logger.debug(f"RELOAD_CONF is {update_time}s")
        except Exception as e:
            logger.error(
                f"Env 'RELOAD_CONF' is invalid '{os.environ.get('RELOAD_CONF')}', set a integer number in seconds ")

        logger.info("Update system config: ended.")
        time.sleep(update_time)
