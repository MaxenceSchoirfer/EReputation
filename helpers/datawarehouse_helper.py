import datetime

import mysql.connector
from jproperties import Properties


class DataWarehouseHelper:

    def __init__(self):
        properties = Properties()
        with open('../app-config.properties', 'rb') as config_file:
            properties.load(config_file)
        self.id_twitter = properties.get("DWH_TWITTER_ID")[0]
        self.mydb = mysql.connector.connect(
            host=properties.get("DWH_HOST")[0],
            user=properties.get("DWH_USER")[0],
            port=properties.get("DWH_PORT")[0],
            password=properties.get("DWH_PASSWORD")[0],
            database=properties.get("DWH_DATABASE_NAME")[0]
        )
        self.cursor = self.mydb.cursor()

    def insert_fact_record_twitter(self, client, date, country, language, sentiment_score, number_like):
        sql = "INSERT INTO fact_record_twitter (id_source, id_client, id_date, id_country, id_language, sentiment_score, number_like) VALUES (%s, %s,%s,%s, %s,%s, %s)"
        values = (self.id_twitter, client, date, country, language, sentiment_score, number_like)
        self.cursor.execute(sql, values)
        self.mydb.commit()

    def insert_fact_frequency(self, source, client, date, country, language, word, number_positive, number_negative,
                              number_neutral,
                              number_total):
        sql = "INSERT INTO fact_frequency (id_source, id_client, id_date, id_country, id_language,  word, number_positive, number_negative,number_neutral, number_total) VALUES (%s, %s,%s,%s, %s,%s, %s,%s, %s, %s)"
        values = (
        source, client, date, country, language, word, number_positive, number_negative, number_neutral, number_total)
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
        result = self.cursor.fetchone()
        if result is None :
            return result
        return result[0]

    def get_id_country(self, alias):
        sql = "SELECT id_country FROM dim_country WHERE alias = %s"
        self.cursor.execute(sql, (alias,))
        result = self.cursor.fetchone()[0]
        return result

    def get_id_language(self, alias):
        sql = "SELECT id_language FROM dim_language WHERE alias = %s"
        self.cursor.execute(sql, (alias,))
        result = self.cursor.fetchone()[0]
        return result

    def create_date_for_year(self, year):
        date = datetime.date(year, 1, 1)
        while date.year == year:
            if self.get_id_date(str(date)) is None :
                self.insert_date(str(date))
            date += datetime.timedelta(days=1)

    def insert_date(self,string):
        sql = "INSERT INTO `db_ereputation`.`date` (`dated`) VALUES (%s);"
        self.cursor.execute(sql, (string,))

