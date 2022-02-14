import boto3
import os
import csv

from jproperties import Properties


class DatalakeHelper:

    def __init__(self):
        properties = Properties()
        with open('../app-config.properties', 'rb') as config_file:
            properties.load(config_file)
        self.client = boto3.client('s3',
                                   aws_access_key_id=properties.get("AWS_ACCESS_KEY_ID"),
                                   aws_secret_access_key=properties.get("AWS_SECRET_ACCESS_KEY"))

    def get_files(self):
        self.client.download_file('e-reputation', 'Twitter/Twitter_CocaCola_18-01-2022_UNK_EN.csv',
                                  'twitter/TWITTER_COCA_18-01-2022_UNK_EN.csv')

    def format_files(self):
        with os.scandir("twitter/") as dirs:
            for entry in dirs:
                print(entry.name)
