import collections
import csv
import re
import time

import enchant
import numpy as np
from nltk import RegexpTokenizer
from nltk.corpus import stopwords
from nltk.corpus import words
from textblob import TextBlob
from pattern.text.en import singularize

import matplotlib.pyplot as plt
import seaborn as sns

# ------------------------------- READING THE CSV FILE -------------------------------------------------
tweets = []

d = enchant.Dict("en_US")

def reading_csv_file(filename, delimiter):
    global tweets
    with open(filename, 'r') as f:
        data_list = list(csv.reader(f, delimiter=delimiter))
    df = np.array(data_list[1:])
    n = len(df)
    # tweets = df[:, :2]
    tweets = df


# ---------------------------------- ANALYSIS FUNCTIONS -------------------------------------------------------
def get_stop_words():
    words = set(stopwords.words('english'))
    personal_stop_word = {"u", "us", "urs", "ur", "cant", "thus", "hes", "shes", "its", "im", "ah", "youve", "yous",
                          "ins",
                          "youll", "dont", "yes", "ya", "ill", "cuz", "btw", "til", "yea", "yeah", "ive", "www", "com",
                          "http", "https", "idk", "yr", "yo", "coz", "bc"
        , "atm", "hadnt", "havent", "didnt", "wouldnt", "arent", "youd", "doesnt", "youre",
                          "hasnt", "wont", "isnt", "com", "eg", "etc", "ex", "hi", "ie", "mr", "mrs", "onto", "one",
                          "two", "three", "four", "five", "six", "seven", "eight", "nine", "cauz",
                          "ten", "eleven", "twelve", "whence", "whereas"}
    for w in personal_stop_word:
        words.add(w)
    return words


# word : [number, negative, neutral, positive]
# negative = 0, neutral = 1, positive = 2
def increment_frequency(frequency, word, sentiment):
    if len(word) < 2:
        return

    if word[-1] == word[-2]:
        while (len(word) > 2) & (word[-1] == word[-2]):

            if not d.check(word):
                word = word[:-1]
            else:
                break
    if not word.endswith("ss"):
        word = singularize(word)


    # # check if not on dict
    # if not word.endswith("ss"):
    #     if not word.endswith("ll"):
    #         while word[-1] == word[-2]:
    #             word = word[:-1]
    #             if len(word) < 2:
    #                 return
    #     word = singularize(word)
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
        if (not re.search('\W', word)) & (not re.search('[0-9]+', word)):
            increment_frequency(frequency, word.lower(), sentiment_polarity)


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

    # confusion_matrix = [[0 for i in range(3)]] * 3
    confusion_matrix = np.zeros((3, 3), dtype=np.int32)

    # -----------------------  ANALYSIS ----------------------------------------

    start = time.time()
    for t_tweet in tweets:
        tweet_id = t_tweet[0]
        tweet_content = t_tweet[1]
        tweet_sentiment = 0
        if t_tweet[2] == "negative":
            tweet_sentiment = 0
        elif t_tweet[2] == "neutral":
            tweet_sentiment = 1
        else:
            tweet_sentiment = 2


        sentiment_score = sentiment_analysis(tweet_content)
        sentiments[tweet_id] = [sentiment_score, 0]

        if sentiment_score < threshold_negative:
            polarity = 0
            n_negative += 1
        elif sentiment_score > threshold_positive:
            polarity = 2
            n_positive += 1
        else:
            polarity = 1
            n_neutral += 1

        confusion_matrix[polarity][tweet_sentiment] += 1

        polarities.append(polarity)
        frequency_analysis(frequency, tokenizer, stop_words, tweet_content, polarity)

    delete = []
    for key, value in frequency.items():
        if value[0] <= 1:
            delete.append(key)

    for key in delete:
        del frequency[key]

    end = time.time()

    # -----------------------  PRINTING ----------------------------------------

    #   plt.figure(figsize=(10, 5))
    #   plt.xlim(-1, 1)
    #   plt.xlabel('Sentiment Score')
    # #  plt.ylim(0, 1)
    #   plt.ylabel('Density')
    #  # sns.kdeplot(np.array(list(sentiments.values()))[:, 0], shade=True)
    #  # sns.distplot(np.array(list(sentiments.values()))[:, 0], bins=100, kde=False)
    #   sns.boxplot(np.array(list(sentiments.values()))[:, 0])
    #   plt.show()
    #
    #   accuracy = np.sum(np.diag(confusion_matrix)) / np.sum(confusion_matrix)
    #   precision_negative = confusion_matrix[0][0] / np.sum(confusion_matrix[0])
    #   precision_neutral = confusion_matrix[1][1] / np.sum(confusion_matrix[1])
    #   precision_positive = confusion_matrix[2][2] / np.sum(confusion_matrix[2])
    #
    #   recall_negative = confusion_matrix[0][0] / np.sum(confusion_matrix[:, 0])
    #   recall_neutral = confusion_matrix[1][1] / np.sum(confusion_matrix[:, 1])
    #   recall_positive = confusion_matrix[2][2] / np.sum(confusion_matrix[:, 2])
    #
    #   print()
    #   print("Sentiment Analysis :", filename)
    print("Analysis Execution Time : ", end - start, "second(s)")
    #   print("Number of tweets analyzed : ", len(sentiments))
    #   print("Number of words on frequency analysis: ", len(frequency))
    #   print()
    #   print("Results :")
    #   print("Negative :", n_negative, "Neutral :", n_neutral, "Positive :", n_positive)
    #   print()
    #   print("Confusion Matrix (line=prediction, column=reality) :\n", confusion_matrix)
    #   print("Accuracy : ", accuracy)
    #   print()
    #   print("Precision Negative : ", precision_negative)
    #   print("Precision Neutral : ", precision_neutral)
    #   print("Precision Positive : ", precision_positive)
    #   print()
    #   print("Recall Negative : ", recall_negative)
    #   print("Recall Neutral : ", recall_neutral)
    #   print("Recall Positive : ", recall_positive)

    for key, values in reversed(sorted(frequency.items(), key=lambda item: item[1])):
        print(key + " : " + str(values[0]))
    print(len(frequency))

    freq = {}
    for key, values in frequency.items():
        if values[0] not in freq:
            freq[values[0]] = 1
        else:
            freq[values[0]] += 1

    plt.figure(figsize=(10, 5))
    # plt.xlim(0, 20)
    plt.xlabel('Number of occurrence')
    #  plt.ylim(0, 1)
    plt.ylabel('Density')
    # sns.kdeplot(np.array(list(sentiments.values()))[:, 0], shade=True)
    # sns.distplot(np.array(list(sentiments.values()))[:, 0], bins=100, kde=False)
    plt.bar(np.array(list(freq.values())), np.array(list(freq.keys())))
    #  sns.boxplot(np.array(list(freq.keys())))
    plt.show()
    od = collections.OrderedDict(sorted(freq.items()))
    print(od)

# ---------------------------------------- RESULT VERIFICATION -----------------------------------------
# if result_verification:
#     with open(filename, 'r') as f:
#         data_list = list(csv.reader(f, delimiter=delimiter))
#     df = np.array(data_list[1:])
#     n = len(df) - 1
#     result = []
#     neg = 0
#     pos = 0
#     neu = 0
#
#     for r in df.T[2][:n]:
#         if r == "neutral":
#             result.append(1)
#             neu += 1
#         elif r == "negative":
#             result.append(0)
#             neg += 1
#         else:
#             result.append(2)
#             pos += 1
#
#     right = 0
#     for i in range(n):
#         if result[i] == polarities[i]:
#             right = right + 1
#
#     percent = right * 100 / n
#     print("Accuracy : " + str(percent) + " %")
#
#     print()
#     print("Expected Results :")
#     print("Negative Tweets :", neg, "Neutral Tweets :", neu, "Positive Tweets :", pos)
#     print()
#     print("Relative Results (results - expected results) :")
#     print("Negative Tweets :", n_negative - neg, "Neutral Tweets :",
#           n_neutral - neu, "Positive Tweets :", n_positive - pos)
#     print()
