import os
import sqlite3

import mysql.connector
from jproperties import Properties


class LocalDBHelper:

    def __init__(self):
        simp_path = 'app-config.properties'
        abs_path = os.path.abspath(simp_path)
        properties = Properties()
        with open(abs_path, 'rb') as config_file:
            properties.load(config_file)
        self.mydb = mysql.connector.connect(
            host=properties.get("DB_SERVER_HOST")[0],
            user=properties.get("DB_SERVER_USER")[0],
            port=properties.get("DB_SERVER_PORT")[0],
            password=properties.get("DB_SERVER_PASSWORD")[0],
            database=properties.get("DB_SERVER_NAME")[0]
        )
        self.cursor = self.mydb.cursor()

    def get_active_clients(self):
        self.cursor.execute("SELECT * FROM client WHERE active = true")
        return self.cursor.fetchall()

    def insert_client(self, alias, keyword, active, dm_name, dm_url, dm_user, dm_port):
        self.cursor.execute(
            "INSERT INTO client (alias, keyword, active, dm_name, dm_url, dm_user, dm_port) VALUES(?, ?,?,?,?,?,?);",
            (alias, keyword, active, dm_name, dm_url, dm_user, dm_port))
        self.mydb.commit()
