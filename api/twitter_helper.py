import csv
import math
import os

import tweepy
import ssl
from datetime import datetime

from jproperties import Properties

# MAX_TWEETS = math.inf
MAX_TWEETS = 10


class TwitterHelper:

    def __init__(self):
        ssl._create_default_https_context = ssl._create_unverified_context

        # Oauth keys
        simp_path = 'app-config.properties'
        abs_path = os.path.abspath(simp_path)
        properties = Properties()
        with open(abs_path, 'rb') as config_file:
            properties.load(config_file)
        self.consumer_key = properties.get("TWITTER_CONSUMER_KEY").data
        self.consumer_secret = properties.get("TWITTER_CONSUMER_SECRET").data
        self.access_token = properties.get("TWITTER_ACCESS_TOKEN").data
        self.access_token_secret = properties.get("TWITTER_ACCESS_TOKEN_SECRET").data
        # Authentication with Twitter
        self.auth = tweepy.OAuthHandler(self.consumer_key, self.consumer_secret)
        self.auth.set_access_token(self.access_token, self.access_token_secret)
        self.api = tweepy.API(self.auth)
        self.api.wait_on_rate_limit = True  # Important pour respecter les limites de la version gratuite

    def generate_csv(self, keyword, alias):
        date = datetime.today().strftime('%Y-%m-%d')
        replies_general = self.get_general_tweets([keyword], 'en')

        simp_path = 'data/twitter/'
        name = '_TWITTER' + '_' + alias + '_' + date + '_UNK' + '_EN' + '.csv'
        abs_path = os.path.abspath(simp_path + name)
        with open(abs_path, 'w', newline='', encoding="utf-8") as f:
            csv_writer = csv.DictWriter(f, fieldnames=['text'])
            csv_writer.writeheader()
            for tweet in replies_general:
                row = {'text': tweet.text.replace('\n', ' ')}
                csv_writer.writerow(row)

    def get_general_tweets(self, keywords, language):
        replies_general = []
        for keyword in keywords:
            for tweet in tweepy.Cursor(self.api.search_tweets, q=keyword).items(MAX_TWEETS):
                if tweet.lang == language:
                    replies_general.append(tweet)

        return replies_general
