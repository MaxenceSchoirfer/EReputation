import mysql.connector

mydb = mysql.connector.connect(
    host="localhost",
    user="yourusername",
    password="yourpassword",
    database="mydatabase"
)

mycursor = mydb.cursor()


def insert_fact_mention(source, client, date, country, is_opinion, positive, negative, number_like):
    sql = "INSERT INTO fact_mentions (id_source, id_client, id_date, id_country, is_opinion, score_positive, score_negative, number_like) VALUES (%s, %s,%s, %s,%s, %s,%s, %s)"
    values = (source, client, date, country, is_opinion, positive, negative, number_like)
    mycursor.execute(sql, values)
    mydb.commit()


insert_fact_mention(1, 1, 1, 1, 1, 0.8, 0, 0)


# to-do
def insert_fact_frequency(source, client, date, country, is_opinion, positive, negative, number_like):
    sql = "INSERT INTO fact_mentions (id_source, id_client, id_date, id_country, is_opinion, score_positive, score_negative, number_like) VALUES (%s, %s,%s, %s,%s, %s,%s, %s)"
    values = (source, client, date, country, is_opinion, positive, negative, number_like)
    mycursor.execute(sql, values)
    mydb.commit()



def get_id_source(name):
    sql = ""
    # name = twitter
    # return SELECT id_source FROM source WHERE name = twitter
    return 0

def get_id_client(name):
    sql = ""
    # name = twitter
    # return SELECT id_source FROM source WHERE name = twitter
    return 0

def get_id_date(name):
    sql = ""
    # name = twitter
    # return SELECT id_source FROM source WHERE name = twitter
    return 0

def get_id_country(name):
    sql = ""
    # name = twitter
    # return SELECT id_source FROM source WHERE name = twitter
    return 0
