import csv
import numpy as np
import re


# format csv filename : SOURCE_CLIENT_mm-dd_yyyy_COUNTRY_LANGUAGE.csv
class Dataset:

    def __init__(self, filename):
        while re.search(r"\A/", filename):
            filename = re.sub(r"\A/", "", filename)
        chunks = re.split("_", filename)
        self.source = re.split("/", chunks[0])[-1]
        self.client = chunks[1]
        self.date = chunks[2]
        self.country = chunks[3]
        self.language = re.split("\.", chunks[4])[0]

        with open(filename, 'r', encoding="utf-8") as f:
            if f != '\n':
                data_list = list(csv.reader(f, delimiter=","))
        df = np.array(data_list[1:])
        self.data = df.T[1][:(len(df) - 1)]
        self.sentiments = []
        self.frequencies = {}
