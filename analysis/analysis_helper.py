from analysis.frequency_helper import FrequencyHelper
from analysis.sentiment_helper import SentimentHelper


def analysis(dataset):
    sentiment_helper = SentimentHelper()
    frequency_helper = FrequencyHelper()

    for content in dataset.data:
        sentiment_score = sentiment_helper.analysis(content)
        dataset.sentiments.append(sentiment_score)
        frequency_helper.analysis(dataset.frequencies, content, sentiment_helper.get_polarity(sentiment_score))
    i = 1
    while len(dataset.frequencies) > 100:
        frequency_helper.remove_low_frequencies(dataset.frequencies, i)
        i += 1

    freq = {}
    for key, values in dataset.frequencies.items():
        if values[0] not in freq:
            freq[values[0]] = 1
        else:
            freq[values[0]] += 1

    return dataset