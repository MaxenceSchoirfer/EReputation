import os
import boto3
from jproperties import Properties


class DatalakeHelper:

    def __init__(self):
        simp_path = 'app-config.properties'
        abs_path = os.path.abspath(simp_path)
        properties = Properties()
        with open(abs_path, 'rb') as config_file:
            properties.load(config_file)
        # self.client = boto3.client('s3',
        #                            aws_access_key_id=properties.get("AWS_ACCESS_KEY_ID"),
        #                            aws_secret_access_key=properties.get("AWS_SECRET_ACCESS_KEY"))
        #
        self.session = boto3.Session(
            aws_access_key_id=properties.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=properties.get("AWS_SECRET_ACCESS_KEY"))

    # todo download all files for specific client on the directory on the data lake
    def download_files(self, client):
        # self.client.download_file('e-reputation', 'Twitter/Twitter_CocaCola_18-01-2022_UNK_EN.csv',
        #                           'twitter/_TWITTER_COCA_2022-10-01_UNK_EN.csv')
        # s3 = self.client('s3')
        s3 = self.session.resource('s3')
        bucket = s3.Bucket('my-reputation')

        #  bucket = self.client.Bucket('my-reputation')
        for obj in bucket.objects.filter(Prefix="Twitter/"):
            # for obj in bucket.objects.filter(Delimiter='/', Prefix='Twitter/'):
            print(obj)

    # todo remove files from datalake
    def remove_files(self, files):
        pass

    # todo return names of files for specific client (stored locally)
    @staticmethod
    def get_filenames(client):
        pass
        # filenames = []
        # with os.scandir(path) as directory:
        #     for file in directory:
        #         filenames.append(file.__getattribute__("name"))
        # return filenames
