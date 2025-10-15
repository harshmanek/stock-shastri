import mysql.connector
from backend.config import DATABASE_CONFIG

def get_db_connection():
    return mysql.connector.connect(**DATABASE_CONFIG)
