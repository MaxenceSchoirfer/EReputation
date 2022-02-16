import time

from analysis.frequency_helper import FrequencyHelper
from analysis.sentiment_helper import SentimentHelper
from helpers.datawarehouse_helper import DataWarehouseHelper
from data.dataset import Dataset


def storage(dataset):
    database_helper = DataWarehouseHelper()
    id_source = database_helper.get_id_source(dataset.source)
    id_client = database_helper.get_id_client(dataset.client)
    id_date = database_helper.get_id_date(dataset.date)
    id_country = database_helper.get_id_country(dataset.country)
    id_language = database_helper.get_id_language(dataset.language)

    for tweet in dataset.sentiments:
        database_helper.insert_fact_record_twitter(id_client, id_date, id_country, id_language, tweet)

    for word_frequency in dataset.frequencies.items():
        total = word_frequency[1][0]
        positive = word_frequency[1][3]
        neutral = word_frequency[1][2]
        negative = word_frequency[1][1]
        database_helper.insert_fact_frequency(id_source, id_client, id_date, id_country, id_language, word_frequency[0],
                                              positive,
                                              negative, neutral, total)


def analysis(filename, header, column, is_test):
    dataset = Dataset(filename, header, column, is_test)
    sentiment_helper = SentimentHelper()
    frequency_helper = FrequencyHelper()

    start = time.time()

    for content in dataset.data:
        sentiment_score = sentiment_helper.analysis(content)
        dataset.sentiments.append(sentiment_score)
        frequency_helper.analysis(dataset.frequencies, content, sentiment_helper.get_polarity(sentiment_score))

    # storage(dataset)
    end = time.time()

    print()
    print("Sentiment Analysis :", filename)
    print("Analysis Execution Time : ", end - start, "second(s)")
