import csv
import re
import time

import numpy as np
from nltk import RegexpTokenizer
from nltk.corpus import stopwords
from textblob import TextBlob

from database.database_helper import DatabaseHelper

# ------------------------------- READING THE CSV FILE -------------------------------------------------


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

def analysis(filename, delimiter, result_verification):
    # ------------------------  INITIALIZATION ----------------------------------------------

    reading_csv_file(filename, delimiter)
    polarities = []  # to compare efficiency

    n_positive = 0
    n_negative = 0
    n_neutral = 0

    threshold_negative = -0.25
    threshold_positive = 0.25

    stop_words = get_stop_words()
    tokenizer = RegexpTokenizer(r'\w+')
    frequency = {}
    sentiments = {}

    # -----------------------  ANALYSIS ----------------------------------------

    start = time.time()
    for t_tweet in tweets:
        tweet_id = t_tweet[0]
        tweet_content = t_tweet[1]
        sentiment_score = sentiment_analysis(tweet_content)
        like_number = 0
        sentiments[tweet_id] = [sentiment_score, like_number]

        if sentiment_score < threshold_negative:
            polarity = 0
            n_negative += 1
        elif sentiment_score > threshold_positive:
            polarity = 2
            n_positive += 1
        else:
            polarity = 1
            n_neutral += 1

        polarities.append(polarity)
        frequency_analysis(frequency, tokenizer, stop_words, tweet_content, polarity)
        end = time.time()

        # -----------------------  PRINTING ----------------------------------------

    print()
    print()
    print("Sentiment Analysis :", filename)
    print("Analysis Execution Time : ", end - start, "second(s)")
    print("Number of tweets analyzed : ", len(sentiments))
    print("Number of words on frequency analysis: ", len(frequency))
    print()
    print("Results :")
    print("Positive :", n_positive, "Negative :", n_negative, "Neutral :", n_neutral)

    # ------------------------------- STORAGE ON DWH -----------------------------------------

    database_helper = DatabaseHelper()
    id_source = database_helper.get_id_source(source)
    id_client = database_helper.get_id_client(client)
    id_date = database_helper.get_id_date(date)
    id_country = database_helper.get_id_country(country)
    id_language = database_helper.get_id_language(language)

    for t in sentiments.items():
        database_helper.insert_fact_record_twitter(id_client, id_date, id_country, id_language, t[1][0], t[1][1])

    for word in frequency.items():
        total = word[1][0]
        positive = word[1][3]
        neutral = word[1][2]
        negative = word[1][1]
        database_helper.insert_fact_frequency(id_source, id_client, id_date, id_country, id_language, word[0],
                                              positive,
                                              negative, neutral, total)

    # ---------------------------------------- RESULT VERIFICATION -----------------------------------------
    if result_verification:
        with open(filename, 'r') as f:
            data_list = list(csv.reader(f, delimiter=delimiter))
        df = np.array(data_list[1:])
        n = len(df) - 1
        result = []
        neg = 0
        pos = 0
        neu = 0

        for r in df.T[2][:n]:
            if r == "neutral":
                result.append(1)
                neu += 1
            elif r == "negative":
                result.append(0)
                neg += 1
            else:
                result.append(2)
                pos += 1

        right = 0
        for i in range(n):
            if result[i] == polarities[i]:
                right = right + 1

        percent = right * 100 / n
        print("Accuracy : " + str(percent) + " %")

        # print()
        # print("Expected Results :")
        # print("Positive Tweets :", pos, "Negative Tweets :", neg, "Neutral Tweets :", neu)
        # print()
        # print("Relative Results (results - expected results) :")
        # print("Positive Tweets :", n_positive - pos, "Negative Tweets :", n_negative - neg, "Neutral Tweets :",
        #       n_neutral - neu)
        # print()
