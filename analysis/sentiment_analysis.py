import csv
import re
import time

import numpy as np
from nltk import RegexpTokenizer
from nltk.corpus import stopwords
from textblob import TextBlob

from database.data_warehouse_helper import DataWarehouseHelper

# ------------------------------- READING THE CSV FILE -------------------------------------------------
from dataset import Dataset

tweets = []
source = ""
client = ""
date = ""
country = ""
language = ""


def reading_csv_file(filename, delimiter):
    global tweets
    global source
    global client
    global date
    global country
    global language

    with open(filename, 'r') as f:
        data_list = list(csv.reader(f, delimiter=delimiter))
    df = np.array(data_list[1:])
    n = len(df)
    tweets = df[:, :2]
    # tweets = df.T[1][:n]

    chunks_properties = re.split("_", filename)
    chunks_path = re.split("/", chunks_properties[0])
    source = chunks_path[-1]
    client = chunks_properties[1]
    date = chunks_properties[2]
    country = chunks_properties[3]
    language = re.sub(".csv", "", chunks_properties[-1])


# ---------------------------------- ANALYSIS FUNCTIONS -------------------------------------------------------
def get_stop_words():
    words = set(stopwords.words('english'))
    return words


# word : [number, negative, neutral, positive]
# negative = 0, neutral = 1, positive = 2
def increment_frequency(frequency, word, sentiment):
    if word in frequency:
        frequency[word][0] = frequency[word][0] + 1
    else:
        frequency[word] = [1, 0, 0, 0]
    frequency[word][sentiment + 1] = frequency[word][sentiment + 1] + 1


def cleanse_tweet(text):
    text = text.encode('ascii', 'ignore').decode('ascii')  # remove non ascii character (emoji/hiding char)
    text = text.lower()
    text = re.sub(r"http\S+", "", text)  # remove link

    # withdraw punctuation at the beginning
    while re.search(r"\A[^a-zA-Z0-9]", text):
        text = re.sub(r"\A[^a-zA-Z0-9]", "", text)
    return text


def sentiment_analysis(text):
    text = cleanse_tweet(text)
    blob = TextBlob(text)
    score = 0  # Polarity of single individual tweet
    for sentence in blob.sentences:
        score += sentence.sentiment.polarity
    return score


def frequency_analysis(frequency, tokenizer, stop_words, text, sentiment_polarity):
    word_tokens = tokenizer.tokenize(text)
    filtered_sentence = [w for w in word_tokens if not w.lower() in stop_words]
    for word in filtered_sentence:
        increment_frequency(frequency, word, sentiment_polarity)


# ------------------------------------ ANALYSIS ----------------------------------------------------------

def analysis(filename):
    # ------------------------  INITIALIZATION ----------------------------------------------

    # reading_csv_file(filename, delimiter)
    dataset = Dataset(filename)

    threshold_negative = -0.25
    threshold_positive = 0.25

    stop_words = get_stop_words()
    tokenizer = RegexpTokenizer(r'\w+')

    # -----------------------  ANALYSIS ----------------------------------------

    start = time.time()
    for content in dataset.data:
        sentiment_score = sentiment_analysis(content)
        if sentiment_score < threshold_negative:
            polarity = 0
            sentiment_score = 0
        elif sentiment_score > threshold_positive:
            polarity = 2
        else:
            polarity = 1

        dataset.sentiments.append(sentiment_score)
        frequency_analysis(dataset.frequencies, tokenizer, stop_words, content, polarity)
        end = time.time()

    # -----------------------  PRINTING ----------------------------------------
    print()
    print("Sentiment Analysis :", filename)
    print("Analysis Execution Time : ", end - start, "second(s)")

    # ------------------------------- STORAGE ON DWH -----------------------------------------

    database_helper = DataWarehouseHelper()
    id_source = database_helper.get_id_source(dataset.source)
    id_client = database_helper.get_id_client(dataset.client)
    id_date = database_helper.get_id_date(dataset.date)
    id_country = database_helper.get_id_country(dataset.country)
    id_language = database_helper.get_id_language(dataset.language)

    for t in dataset.sentiments:
        database_helper.insert_fact_record_twitter(id_client, id_date, id_country, id_language, t, 0)

    for word_frequency in dataset.frequencies.items():
        total = word_frequency[1][0]
        positive = word_frequency[1][3]
        neutral = word_frequency[1][2]
        negative = word_frequency[1][1]
        database_helper.insert_fact_frequency(id_source, id_client, id_date, id_country, id_language, word_frequency[0],
                                              positive,
                                              negative, neutral, total)
