import collections
import time

from analysis.frequency_helper import FrequencyHelper
from analysis.sentiment_helper import SentimentHelper
from data.dataset import Dataset
from helpers.datawarehouse_helper import DataWarehouseHelper


def analysis(dataset):
    # dataset = Dataset(filename, is_test)
    sentiment_helper = SentimentHelper()
    frequency_helper = FrequencyHelper()

    start = time.time()

    for content in dataset.data:
        sentiment_score = sentiment_helper.analysis(content)
        dataset.sentiments.append(sentiment_score)
        frequency_helper.analysis(dataset.frequencies, content, sentiment_helper.get_polarity(sentiment_score))
    #   frequency_helper.remove_low_frequencies(dataset.frequencies, 5)
    i = 1
    while len(dataset.frequencies) > 100:
        frequency_helper.remove_low_frequencies(dataset.frequencies, i)
        i += 1
    # print(i)

    freq = {}
    for key, values in dataset.frequencies.items():
        if values[0] not in freq:
            freq[values[0]] = 1
        else:
            freq[values[0]] += 1
    od = collections.OrderedDict(sorted(freq.items()))
    # print(od)

    # for key, values in reversed(sorted(dataset.frequencies.items(), key=lambda item: item[1])):
    #     if True:
    #         # print(key + " : " + str(values[0]))
    # print(len(dataset.frequencies))

    end = time.time()
    return dataset

    # print()
    # print("Sentiment Analysis :", dataset.)
    # print("Analysis Execution Time : ", end - start, "second(s)")
