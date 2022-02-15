import os

import boto3
from jproperties import Properties


def get_filenames(path):
    filenames = []
    with os.scandir(path) as directory:
        for file in directory:
            filenames.append(file.__getattribute__("name"))
    return filenames

class DatalakeHelper:

    def __init__(self):
        properties = Properties()
        with open('../app-config.properties', 'rb') as config_file:
            properties.load(config_file)
        self.client = boto3.client('s3',
                                   aws_access_key_id=properties.get("AWS_ACCESS_KEY_ID"),
                                   aws_secret_access_key=properties.get("AWS_SECRET_ACCESS_KEY"))

    # todo get all files on the directory on the data lake
    def get_files(self):
        self.client.download_file('e-reputation', 'Twitter/Twitter_CocaCola_18-01-2022_UNK_EN.csv',
                                  'twitter/TWITTER_COCA_2022-10-01_UNK_EN.csv')
