import os
import sqlite3


class LocalDBHelper:

    def __init__(self):
        simp_path = 'database/local.db'
        abs_path = os.path.abspath(simp_path)
        self.connection = sqlite3.connect(abs_path)
        self.cursor = self.connection.cursor()

    def create_db(self):
        self.cursor.execute('''CREATE TABLE client
                       (id_client integer primary key autoincrement, alias text,keyword text, active bool)''')

    def get_active_clients(self):
        self.cursor.execute("SELECT * FROM client WHERE active = true")
        return self.cursor.fetchall()

    def insert_client(self, alias, keyword, active):
        self.cursor.execute("INSERT INTO client (id_client, alias, keyword, active) VALUES (null,?,?,?)",
                            (alias, keyword, active))  # % name, alias, active)
        self.connection.commit()
