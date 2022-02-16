import csv
import tweepy
import ssl
from datetime import datetime

from jproperties import Properties

ssl._create_default_https_context = ssl._create_unverified_context
MAX_TWEETS = 5000000000000000000000

# Oauth keys
properties = Properties()
with open('../app-config.properties', 'rb') as config_file:
    properties.load(config_file)

consumer_key = properties.get("TWITTER_CONSUMER_KEY")
consumer_secret = properties.get("TWITTER_CONSUMER_SECRET")
access_token = properties.get("TWITTER_ACCESS_TOKEN")
access_token_secret = properties.get("TWITTER_ACCESS_TOKEN_SECRET")

# Authentication with Twitter
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)
api.wait_on_rate_limit = True  # Important pour respecter les limites de la version gratuite

name = 'CocaCola'
alias = 'COCA'
user = api.get_user(screen_name=name)
follower_count = user.followers_count
date = datetime.today().strftime('%m-%d-%Y')

#Not used
def hashtag(list):
    replies_hashtag = []
    HashtagCount = 0
    count = 0  # A supprimer
    count2 = 0
    for keyword in list:
        for tweet in tweepy.Cursor(api.search_tweets, q='#' + keyword).items(MAX_TWEETS):
            if tweet.lang == 'en':
                replies_hashtag.append(tweet)
                if tweet.geo: count += 1
                if tweet.place: count2 += 1
    print('Nombre de coordonnées :' + str(count))
    print('Nombre de Places :' + str(count2))
    return replies_hashtag, len(replies_hashtag)

def page_scoped(brand):
    replies_page = []
    for tweet in tweepy.Cursor(api.search_tweets, q='to:' + brand).items(MAX_TWEETS):
        if hasattr(tweet,
                   'in_reply_to_status_id_str'):  # seul les tweets qui répondent directement au client sont pris en compte
            if tweet.lang == 'en':
                replies_page.append(tweet)
    return replies_page, len(replies_page)

def general_tweets(list):
    replies_general = []
    for keyword in list:
        for tweet in tweepy.Cursor(api.search_tweets, q=keyword).items(MAX_TWEETS):
            if tweet.lang == 'en':
                replies_general.append(tweet)
    return replies_general, len(replies_general)

def call_hashtag():
    replies_hashtag, Hashtag_count = hashtag(['CocaCola', 'Cocacola'])
    with open(
            'C:/Users/Bouha/Desktop/E-reputation/Resultat CSV/HASHTAG_TWITTER' + '_' + alias + '_' + date + '_UNK' + '_EN' + '.csv',
            'w', encoding="utf-8") as f:
        csv_writer = csv.DictWriter(f, fieldnames=('User', 'Text', 'Time', 'Language', 'Coordonnées', 'Place'))
        csv_writer.writeheader()
        for tweet in replies_hashtag:
            row = {'User': tweet.user.screen_name, 'Text': tweet.text.replace('\n', ' '), 'Time': tweet.created_at,
                   'Language': tweet.lang, 'Coordonnées': tweet.geo, 'Place': tweet.place}

def call_page():
    replies_page, page_tweet_count = page_scoped(name)
    with open(
            'C:/Users/Bouha/Desktop/E-reputation/Resultat CSV/PAGE_TWITTER' + '_' + alias + '_' + date + '_UNK' + '_EN' + '.csv',
            'w', encoding="utf-8") as f:
        csv_writer = csv.DictWriter(f, fieldnames=('User', 'Text', 'Time', 'Language'))
        csv_writer.writeheader()
        for tweet in replies_page:
            row = {'User': tweet.user.screen_name, 'Text': tweet.text.replace('\n', ' '), 'Time': tweet.created_at,
                   'Language': tweet.lang}
            csv_writer.writerow(row)

def call_general():
    replies_general, general_tweet_count = general_tweets(['Cocacola'])
    with open(
            'C:/Users/Bouha/Desktop/E-reputation/Resultat CSV/TWITTER' + '_' + alias + '_' + date + '_UNK' + '_EN' + '.csv',
            'w', encoding="utf-8") as f:
        csv_writer = csv.DictWriter(f, fieldnames=('User', 'Text', 'Time', 'Language'))
        csv_writer.writeheader()
        for tweet in replies_general:
            row = {'User': tweet.user.screen_name, 'Text': tweet.text.replace('\n', ' '), 'Time': tweet.created_at,
                   'Language': tweet.lang}
            csv_writer.writerow(row)


call_general()