import csv
import re
import time

import numpy as np
from nltk import RegexpTokenizer
from nltk.corpus import stopwords
from textblob import TextBlob


def get_tweets(file_name, delimiter):
    with open(file_name, 'r') as f:
        data_list = list(csv.reader(f, delimiter=delimiter))
    df = np.array(data_list[1:])
    n = len(df) - 1
    return df.T[1][:n]


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


def analysis(file_name, delimiter, result_verification):
    tweets = get_tweets(file_name, delimiter)
    sentiments = []
    polarities = []  # to compare efficiency

    n_positive = 0
    n_negative = 0
    n_neutral = 0

    threshold_negative = -0.25
    threshold_positive = 0.25

    stop_words = get_stop_words()
    tokenizer = RegexpTokenizer(r'\w+')
    frequency = {}

    start = time.time()
    for tweet in tweets:
        sentiment_score = sentiment_analysis(tweet)
        sentiments.append(sentiment_score)
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
        frequency_analysis(frequency, tokenizer, stop_words, tweet, polarity)
    end = time.time()

    print()
    print()
    print("Sentiment Analysis :", file_name)
    print("Analysis Execution Time : ", end - start, "second(s)")
    print("Number of tweets analyzed : ", len(sentiments))
    print("Number of words on frequency analysis: ", len(frequency))
    print()
    print("Results :")
    print("Positive :", n_positive, "Negative :", n_negative, "Neutral :", n_neutral)

    # ---------------------------------------- RESULT VERIFICATION -----------------------------------------
    if result_verification:
        with open(file_name, 'r') as f:
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

        details = False
        if details:
            print()
            print("Expected Results :")
            print("Positive Tweets :", pos, "Negative Tweets :", neg, "Neutral Tweets :", neu)
            print()
            print("Relative Results (results - expected results) :")
            print("Positive Tweets :", n_positive - pos, "Negative Tweets :", n_negative - neg, "Neutral Tweets :",
                  n_neutral - neu)
            print()


#analysis("data/test.csv", ";", True)
