import csv
import numpy as np
import re


# format csv filename : SOURCE_CLIENT_mm-dd_yyyy_COUNTRY_LANGUAGE.csv
class Dataset:

    def __init__(self, filename, has_header, id_data_column, is_test_file):
        if not is_test_file:
            # ------- RECOVER METADATA -------------------------
            while re.search(r"\A/", filename):
                filename = re.sub(r"\A/", "", filename)
            chunks = re.split("_", filename)
            self.source = re.split("/", chunks[0])[-1]
            self.client = chunks[1]
            self.date = chunks[2]
            self.country = chunks[3]
            self.language = re.split("\.", chunks[4])[0]

        # ---------------- EXTRACT DAT --------------------------
        with open(filename, 'r', encoding="utf-8") as f:
            data_list = list(csv.reader(f, delimiter=";"))
        if has_header:
            data_list.pop(0)
        for line in data_list:
            if not line:
                data_list.remove(line)
        self.data = np.array(data_list)[:, id_data_column]
        self.sentiments = []
        self.frequencies = {}
