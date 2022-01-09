import mysql.connector
from jproperties import Properties


class DatabaseHelper:

    def __init__(self):
        properties = Properties()
        with open('app-config.properties', 'rb') as config_file:
            properties.load(config_file)
        self.id_twitter = properties.get("id_source_twitter")[0]
        self.mydb = mysql.connector.connect(
            host=properties.get("host")[0],
            user=properties.get("user")[0],
            password=properties.get("password")[0],
            database=properties.get("database")[0]
            # host="localhost",
            # user="root",
            # password="",
            # database="db_ereputation"
        )
        self.cursor = self.mydb.cursor()

    def insert_fact_record_twitter(self, client, date, country, sentiment_score, number_like):
        sql = "INSERT INTO fact_record_twitter (id_source, id_client, id_date, id_country, sentiment_score, number_like) VALUES (%s, %s,%s, %s,%s, %s)"
        values = (self.id_twitter, client, date, country, sentiment_score, number_like)
        self.cursor.execute(sql, values)
        self.mydb.commit()

    def insert_fact_frequency(self, source, client, date, country, word, number_positive, number_negative, number_neutral,
                              number_total):
        sql = "INSERT INTO fact_frequency (id_source, id_client, id_date, id_country,  word, number_positive, number_negative,number_neutral, number_total) VALUES (%s, %s,%s, %s,%s, %s,%s, %s, %s)"
        values = (source, client, date, country, word, number_positive, number_negative, number_neutral, number_total)
        self.cursor.execute(sql, values)
        self.mydb.commit()

    def get_id_source(self, alias):
        sql = "SELECT id_source FROM dim_source WHERE alias = %s"
        self.cursor.execute(sql, (alias,))
        result = self.cursor.fetchone()[0]
        return result

    def get_id_client(self, alias):
        sql = "SELECT id_client FROM dim_client WHERE alias = %s"
        self.cursor.execute(sql, (alias,))
        result = self.cursor.fetchone()[0]
        return result

    def get_id_date(self, date):
        sql = "SELECT id_date FROM date WHERE dated = %s"
        self.cursor.execute(sql, (date,))
        result = self.cursor.fetchone()[0]
        return result

    def get_id_country(self, alias):
        sql = "SELECT id_country FROM dim_country WHERE alias = %s"
        self.cursor.execute(sql, (alias,))
        result = self.cursor.fetchone()[0]
        return result
