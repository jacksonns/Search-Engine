import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize 
from nltk.stem import RSLPStemmer
import string

nltk.download('punkt')
nltk.download('stopwords')
nltk.download('rslp')

STEMMER = RSLPStemmer()
STOP_WORDS = set(stopwords.words('portuguese') + list(string.punctuation))

class Tokenizer:

    def tokenize(body):
        try:
            tokens = word_tokenize(body)
            processed_tokens = [STEMMER.stem(word) for word in tokens if (not word in STOP_WORDS and 
                                                                        len(word) < 25 and 
                                                                        word < 'zzz')]
            return processed_tokens
        except:
            return []
    
    # Remove stop words and do stemming. Returns pairs (token, frequency) 
    def get_token_freq_list(body):
        processed_tokens = Tokenizer.tokenize(body)
        frequency = nltk.FreqDist(processed_tokens)
        return [(word, freq) for word, freq in frequency.items()], len(processed_tokens)