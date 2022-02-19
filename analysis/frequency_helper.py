import re
import enchant
import nlp as nlp
from nltk import RegexpTokenizer
from nltk.corpus import stopwords
from pattern.text.en import singularize
from nltk.stem import WordNetLemmatizer

import spacy


def get_stopwords():
    words = set(stopwords.words('english'))
    with open('stopwords.txt') as f:
        lines = f.readlines()
        for w in lines:
            words.add(re.sub("\n", "", w))
    return words


# word : [number, negative, neutral, positive]
# negative = 0, neutral = 1, positive = 2
def increment_frequency(frequency, word, sentiment):
    if word in frequency:
        frequency[word][0] = frequency[word][0] + 1
    else:
        frequency[word] = [1, 0, 0, 0]
    frequency[word][sentiment + 1] = frequency[word][sentiment + 1] + 1


class FrequencyHelper:

    def __init__(self):
        self.english_dictionary = enchant.Dict("en_US")
        self.stopwords = get_stopwords()
        self.tokenizer = RegexpTokenizer(r'\w+')
        self.lemmatizer = WordNetLemmatizer()

    # return a clean word or None
    def check_frequency_validity(self, word):
        if (re.search("[0-9]+", word)) or (re.search("[_]+", word)):
            return None

        if len(word) < 2:
            return None

        if word[-1] == word[-2]:
            while (len(word) > 2) & (word[-1] == word[-2]):
                if not self.english_dictionary.check(word):
                    word = word[:-1]
                else:
                    break
            if len(word) < 2:
                return None
        if not word.endswith("ss"):
            word = singularize(word)

        return word

    def analysis(self, frequency, text, sentiment_polarity):
        text = text.lower()
        word_tokens = self.tokenizer.tokenize(text)
        filtered_sentence = [w for w in word_tokens if not w.lower() in self.stopwords]
        for word in filtered_sentence:
            word = self.check_frequency_validity(word)
            if word is not None:
                word = self.lemmatizer.lemmatize(word, "v")
                increment_frequency(frequency, word, sentiment_polarity)

    @staticmethod
    def remove_low_frequencies(frequency, threshold):
        delete = []
        for key, value in frequency.items():
            if value[0] <= threshold:
                delete.append(key)

        for key in delete:
            del frequency[key]

    @staticmethod
    def join_word(frequency):
        nlp = spacy.load('en_core_web_sm')
        new_frequency = {}
        withdraw = []
        for key1, value1 in frequency.items():
            for key2, value2 in frequency.items():
                if not withdraw.__contains__(key1) and not withdraw.__contains__(key2):
                    if nlp(key1).similarity(nlp(key2)) > 0.85:
                        print(key1, ",", key2)
                        new_value = [value1[0] + value2[0], value1[1] + value2[1], value1[2] + value2[2],
                                     value1[3] + value2[3]]
                        if value1 > value2:
                            withdraw.append(key2)
                            if not new_frequency.__contains__(key1):
                                new_frequency[key1] = new_value
                            else:
                                new_frequency[key1] = [new_frequency[key1][0] + value2[0],
                                                       new_frequency[key1][1] + value2[1],
                                                       new_frequency[key1][2] + value2[2],
                                                       new_frequency[key1][3] + value2[3]]
                        else:
                            withdraw.append(key1)
                            if not new_frequency.__contains__(key2):
                                new_frequency[key2] = new_value
                            else:
                                new_frequency[key2] = [new_frequency[key2][0] + value1[0],
                                                       new_frequency[key2][1] + value1[1],
                                                       new_frequency[key2][2] + value1[2],
                                                       new_frequency[key2][3] + value1[3]]
            new_frequency[key1] = value1

        return new_frequency
