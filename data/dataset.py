import csv
import numpy as np
import re


# format csv filename : SOURCE_CLIENT_mm-dd_yyyy_COUNTRY_LANGUAGE.csv
class Dataset:

    def __init__(self, filename, is_test_file):
        if not is_test_file:
            # ------- RECOVER METADATA -------------------------
            while re.search(r"\A/", filename):
                filename = re.sub(r"\A/", "", filename)
            chunks = re.split("_", filename)
            self.source = chunks[1]
            self.client = chunks[2]
            self.date = chunks[3]
            self.country = chunks[4]
            self.language = re.split("\.", chunks[5])[0]
            self.file = filename

        # ---------------- EXTRACT DAT --------------------------
        with open(filename, 'r', errors="ignore") as f:
            # data_list = list(csv.reader(f, delimiter=";"))
            reader = csv.DictReader(f, delimiter=";")
            data = []
            for row in reader:
                data.append(row["text"])
        self.data = np.array(data)
        self.sentiments = []
        self.frequencies = {}
