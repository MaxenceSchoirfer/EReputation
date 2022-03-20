import os
import re

import boto3
from jproperties import Properties


class DatalakeHelper:

    def __init__(self):
        simp_path = 'app-config.properties'
        abs_path = os.path.abspath(simp_path)
        properties = Properties()
        with open(abs_path, 'rb') as config_file:
            properties.load(config_file)
        self.client = boto3.client('s3',
                                   aws_access_key_id=properties.get("AWS_ACCESS_KEY_ID"),
                                   aws_secret_access_key=properties.get("AWS_SECRET_ACCESS_KEY"))

    def upload_file(self, path):
        local_path = os.path.abspath(path)
        chunks = re.split("/", path)
        datalake_path = 'Twitter/' + str(chunks[-1])
        self.client.upload_file(local_path, 'e-reputation', datalake_path)
        #self.client.upload_file('C://Dev//ERep//data//twitter//_TWITTER_COCA_2022-02-25_UNK_EN.csv', 'e-reputation',
           #                     'Twitter/_TWITTER_COCA_2022-02-25_UNK_EN.csv')

    @staticmethod
    def get_local_filenames(alias, date):
        simp_path = 'data/twitter'
        abs_path = os.path.abspath(simp_path)
        filenames = []
        with os.scandir(abs_path) as directory:
            for file in directory:
                if str(file).__contains__(alias) and str(file).__contains__(date):
                    filenames.append(simp_path + "/" + file.__getattribute__("name"))
        return filenames
