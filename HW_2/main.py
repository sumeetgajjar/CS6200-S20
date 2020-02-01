from HW_2.stopwords import StopwordsFilter
from HW_2.tokenizer import Tokenzier
from utils.utils import Utils

if __name__ == '__main__':
    text = "The car was in the car wash."
    tokenizer = Tokenzier()
    tokens = tokenizer.tokenize(text)
    stopwords_filter = StopwordsFilter(Utils.get_stopwords_file_path())
    stopwords_filter.filter(tokens)
