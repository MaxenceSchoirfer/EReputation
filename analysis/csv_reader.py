import csv
import numpy as np
import re


def get_tweets(filename, delimiter):
    with open(filename, 'r') as f:
        data_list = list(csv.reader(f, delimiter=delimiter))
    df = np.array(data_list[1:])
    n = len(df) - 1
    return df.T[1][:n]


def get_source(filename):
    while re.search(r"\A/", filename):
        filename = re.sub(r"\A/", "",filename)
    chunks = re.split("_", filename)
    return chunks[0]

def get_date(filename):
    chunks = re.split("_", filename)
    return chunks[1]

def get_country(filename):
    chunks = re.split("_", filename)
    return chunks[2]

def get_language(filename):
    chunks = re.split("_", filename)
    return chunks[3]