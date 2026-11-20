import psycopg2
from psycopg2 import Error
from loguru import logger
from config import load_config

def connect_db():
    """Establish a connection to the PostgreSQL database via the SSH tunnel."""
    try:
        logger.info("Establishing database connection...")
        db_config = load_config().db
        connection = psycopg2.connect(
            host=db_config.local_host,
            port=db_config.local_port,
            user=db_config.username,
            password=db_config.password,
            database=db_config.database
        )
        
        cursor = connection.cursor()
        logger.info("Database connection established successfully.")

        return connection, cursor
    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")
        raise Exception(f"Error setting up connection: {e}")
    except Exception as e:
        logger.error(f"General error: {e}")
        raise Exception(f"Error setting up connection: {e}")

def close_db(connection=None, cursor=None):
    """Close the database connection and cursor, if open."""
    if cursor:
        cursor.close()
        logger.info("Database cursor closed.")
    
    if connection:
        connection.close()
        logger.info("Database connection closed.")
