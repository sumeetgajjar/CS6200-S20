from nltk.stem import SnowballStemmer

from HW_1.main import get_file_paths_to_parse, get_parsed_documents
from HW_2.index import CustomIndex
from HW_2.stopwords import StopwordsFilter
from HW_2.tokenizer import Tokenzier
from utils.utils import Utils


class HW2:

    @classmethod
    def create_batches(cls):
        dir_path = Utils.get_ap89_collection_abs_path()
        file_paths = get_file_paths_to_parse(dir_path)
        return Utils.split_list_into_sub_lists(file_paths, 1000)

    @classmethod
    def add_documents_to_index(cls, documents, index_head=False, enable_stemming=False):
        tokenizer = Tokenzier()
        stopwords_filter = StopwordsFilter(Utils.get_stopwords_file_path())
        stemmer = SnowballStemmer('english')
        index = CustomIndex(tokenizer, stopwords_filter, stemmer)
        index.add_documents(documents, index_head, enable_stemming)

    @classmethod
    def main(cls):
        for batch in cls.create_batches():
            parsed_documents = get_parsed_documents(batch)
            cls.add_documents_to_index(parsed_documents)


if __name__ == '__main__':
    HW2.main()
