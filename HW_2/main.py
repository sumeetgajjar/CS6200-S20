import logging

from nltk.stem import SnowballStemmer

from HW_1.main import get_file_paths_to_parse, get_parsed_documents
from HW_2.index import CustomIndex
from HW_2.stopwords import StopwordsFilter
from HW_2.tokenizer import Tokenzier
from utils.utils import Utils


class HW2:

    @classmethod
    def create_files_to_read_batches(cls):
        dir_path = Utils.get_ap89_collection_abs_path()
        file_paths = get_file_paths_to_parse(dir_path)
        logging.info("Total File to read: {}".format(len(file_paths)))
        return Utils.split_list_into_sub_lists(file_paths, no_of_sub_lists=20)

    @classmethod
    def add_documents_to_index(cls, documents, index_head=False, enable_stemming=False):
        tokenizer = Tokenzier()
        stopwords_filter = StopwordsFilter(Utils.get_stopwords_file_path())
        stemmer = SnowballStemmer('english')
        index = CustomIndex(tokenizer, stopwords_filter, stemmer)
        index.add_documents(documents, index_head, enable_stemming)

    @classmethod
    def main(cls):
        Utils.configure_logging()
        for file_batch in cls.create_files_to_read_batches():
            parsed_documents = get_parsed_documents(file_batch)
            for documents_batch in Utils.split_list_into_sub_lists(parsed_documents, sub_list_size=1000):
                cls.add_documents_to_index(documents_batch)


if __name__ == '__main__':
    HW2.main()
