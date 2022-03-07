import mysql.connector
from jproperties import Properties

from helpers.datawarehouse_helper import DataWarehouseHelper


class DataMartHelper:

    def __init__(self, alias, db_name, db_url, db_user, port):
        self.i = 0
        properties = Properties()
        with open('app-config.properties', 'rb') as config_file:
            properties.load(config_file)
        self.id_twitter = properties.get("DWH_TWITTER_ID")[0]
        # self.mydb = mysql.connector.connect(
        #     host=properties.get("DM_COCA_HOST")[0],
        #     user=properties.get("DM_COCA_USER")[0],
        #     port=properties.get("DM_COCA_PORT")[0],
        #     password=properties.get("DM_COCA_PASSWORD")[0],
        #     database=properties.get("DM_COCA_DATABASE_NAME")[0]
        # )
        field_name = db_name + "_password"
        self.mydb = mysql.connector.connect(
            host=db_url,
            user=db_user,
            port=port,
            password=properties.get(field_name.upper())[0],
            database=db_name
        )
        self.cursor = self.mydb.cursor()
        self.dwh = DataWarehouseHelper()
        self.id_client = self.dwh.get_id_client(alias)

    def create_dim_from_dwh(self):
        self.__create_dates_from_dwh()
        self.__create_countries_from_dwh()
        self.__create_languages_from_dwh()
        self.__create_sources_from_dwh()

    def insert_fact_frequency(self, id_date):
        for line in self.dwh.get_fact_frequency(self.id_client, id_date):
            sql = "INSERT INTO fact_frequency (id_fact_frequency, id_source,id_date,id_country,id_language,word,number_positive,number_negative,number_neutral,number_total) " \
                  "VALUES( %s, %s, %s,%s,%s,%s,%s,%s,%s,%s)"
            values = (line[0], line[1], line[3], line[4], line[5], line[6], line[7], line[8], line[9], line[10])
            self.cursor.execute(sql, values)
            self.mydb.commit()

    def insert_fact_record_twitter(self, id_date):
        for line in self.dwh.get_fact_record_twitter(self.id_client, id_date):
            sql = "INSERT INTO fact_record_twitter (id_fact_record_twitter, id_source,id_date,id_country,id_language,sentiment_score) " \
                  "VALUES( %s, %s, %s,%s,%s,%s)"
            values = (line[0], line[1], line[3], line[4], line[5], line[6])
            self.cursor.execute(sql, values)
            self.mydb.commit()

    def __create_dates_from_dwh(self):
        self.cursor.execute("TRUNCATE date;")
        self.mydb.commit()
        self.cursor.execute("ALTER TABLE date AUTO_INCREMENT = 1;")
        self.mydb.commit()
        for line in self.dwh.get_all_dates():
            sql = "INSERT INTO date (id_date, dated) VALUES (%s, %s)"
            values = (line[0], str(line[1]))
            self.cursor.execute(sql, values)
            self.mydb.commit()

    def __create_countries_from_dwh(self):
        self.cursor.execute("TRUNCATE dim_country;")
        self.mydb.commit()
        self.cursor.execute("ALTER TABLE dim_country AUTO_INCREMENT = 1;")
        self.mydb.commit()
        for line in self.dwh.get_all_countries():
            sql = "INSERT INTO dim_country (id_country, name,alias) VALUES (%s, %s,%s)"
            values = (line[0], line[1], line[2])
            self.cursor.execute(sql, values)
            self.mydb.commit()

    def __create_languages_from_dwh(self):
        self.cursor.execute("TRUNCATE dim_language;")
        self.mydb.commit()
        self.cursor.execute("ALTER TABLE dim_language AUTO_INCREMENT = 1;")
        self.mydb.commit()
        for line in self.dwh.get_all_languages():
            sql = "INSERT INTO dim_language (id_language, name,alias) VALUES (%s, %s,%s)"
            values = (line[0], line[1], line[2])
            self.cursor.execute(sql, values)
            self.mydb.commit()

    def __create_sources_from_dwh(self):
        self.cursor.execute("TRUNCATE dim_source;")
        self.mydb.commit()
        self.cursor.execute("ALTER TABLE dim_source AUTO_INCREMENT = 1;")
        self.mydb.commit()
        for line in self.dwh.get_all_sources():
            sql = "INSERT INTO dim_source (id_source, name,alias) VALUES (%s, %s,%s)"
            values = (line[0], line[1], line[2])
            self.cursor.execute(sql, values)
            self.mydb.commit()
