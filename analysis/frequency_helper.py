import re
import enchant
from nltk import RegexpTokenizer
from nltk.corpus import stopwords
from pattern.text.en import singularize


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

    # return a clean word or None
    def check_frequency_validity(self, word):
        if len(word) < 2:
            return None

        if word[-1] == word[-2]:
            while (len(word) > 2) & (word[-1] == word[-2]):
                if not self.english_dictionary.check(word):
                    word = word[:-1]
                else:
                    break
        if not word.endswith("ss"):
            word = singularize(word)
        return word

    def analysis(self, frequency, text, sentiment_polarity):
        word_tokens = self.tokenizer.tokenize(text)
        filtered_sentence = [w for w in word_tokens if not w.lower() in self.stopwords]
        for word in filtered_sentence:
            word = self.check_frequency_validity(word)
            if word is not None:
                increment_frequency(frequency, word, sentiment_polarity)



