import mysql.connector

mydb = mysql.connector.connect(
    host="localhost",
    user="yourusername",
    password="yourpassword",
    database="mydatabase"
)

mycursor = mydb.cursor()


def insert_fact_mention(source, client, date, country, sentiment_score, number_like):
    sql = "INSERT INTO fact_mentions (id_source, id_client, id_date, id_country, sentiment_score, number_like) VALUES (%s, %s,%s, %s,%s, %s)"
    values = (source, client, date, country, sentiment_score, number_like)
    mycursor.execute(sql, values)
    mydb.commit()


def insert_fact_frequency(source, client, date, country, word, number_positive, number_negative, number_neutral,
                          number_total):
    sql = "INSERT INTO fact_frequency (id_source, id_client, id_date, id_country,  word, number_positive, number_negative,number_neutral, number_total) VALUES (%s, %s,%s, %s,%s, %s,%s, %s, %s)"
    values = (source, client, date, country, word, number_positive, number_negative, number_neutral, number_total)
    mycursor.execute(sql, values)
    mydb.commit()


# --------------------


def get_id_source(name):
    sql = "SELECT * FROM source WHERE name = %s"
    mycursor.execute(sql, name)
    myresult = mycursor.fetchall()
    ids_list = []
    for x in myresult:
        ids_list.append(x[0])
    return ids_list[0]


# --------------------


def get_id_client(name):
    sql = "SELECT * FROM client WHERE name = %s"
    # name = ("twitter", )
    mycursor.execute(sql, name)
    myresult = mycursor.fetchall()
    ids_list = []
    for x in myresult:
        ids_list.append(x[0])
    return ids_list[0]


# --------------------

# 22/01/2021
def get_id_date(date):
    sql = "SELECT * FROM date WHERE dated = %s"
    mycursor.execute(sql, date)
    myresult = mycursor.fetchall()
    return myresult[0]


# --------------------


def get_id_country(name):
    sql = "SELECT * FROM country WHERE name = %s"
    name = ("twitter",)
    mycursor.execute(sql, name)
    myresult = mycursor.fetchall()
    ids_list = []
    for x in myresult:
        ids_list.append(x[0])
    return ids_list
