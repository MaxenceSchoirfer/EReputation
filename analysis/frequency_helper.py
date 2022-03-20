import os
import re
import enchant
from nltk import RegexpTokenizer
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer


def get_stopwords():
    simp_path = 'analysis/stopwords.txt'
    abs_path = os.path.abspath(simp_path)
    words = set(stopwords.words('english'))
    with open(abs_path) as f:
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

        return word

    def analysis(self, frequency, text, sentiment_polarity):
        text = text.lower()
        text = re.sub(r"http\S+", "", text)  # remove link
        text = text.encode('ascii', 'ignore').decode('ascii')  # remove non ascii character (emoji/hiding char)
        word_tokens = self.tokenizer.tokenize(text)
        filtered_sentence = [w for w in word_tokens if not w.lower() in self.stopwords]

        for word in filtered_sentence:
            word = self.check_frequency_validity(word)
            if word is not None:
                word = self.lemmatizer.lemmatize(word, "v")
                word = self.lemmatizer.lemmatize(word, "a")
                increment_frequency(frequency, word, sentiment_polarity)

    @staticmethod
    def remove_low_frequencies(frequency, threshold):
        delete = []
        for key, value in frequency.items():
            if value[0] <= threshold:
                delete.append(key)

        for key in delete:
            del frequency[key]
