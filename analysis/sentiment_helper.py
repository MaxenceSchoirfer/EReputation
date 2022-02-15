import re

from textblob import TextBlob


def cleanse_tweet(text):
    text = text.encode('ascii', 'ignore').decode('ascii')  # remove non ascii character (emoji/hiding char)
    text = text.lower()
    text = re.sub(r"http\S+", "", text)  # remove link

    # withdraw punctuation at the beginning
    while re.search(r"\A[^a-zA-Z0-9]", text):
        text = re.sub(r"\A[^a-zA-Z0-9]", "", text)
    return text


class SentimentHelper:

    def __init__(self):
        self.threshold_negative = -0.25
        self.threshold_positive = 0.25

    def analysis(self, text):
        text = cleanse_tweet(text)
        blob = TextBlob(text)
        score = 0  # Polarity of single individual tweet
        for sentence in blob.sentences:
            score += sentence.sentiment.polarity
        return score

    def get_polarity(self, sentiment_score):
        if sentiment_score < self.threshold_negative:
            return 0
        elif sentiment_score > self.threshold_positive:
            return 2
        else:
            return 1
