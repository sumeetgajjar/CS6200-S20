import logging

from HW_1.main import get_file_paths_to_parse, get_parsed_documents
from HW_2.factory import Factory
from HW_2.indexer import CustomIndex
from constants.constants import Constants
from utils.decorators import timing
from utils.utils import Utils


class HW2:

    @classmethod
    def create_files_to_read_batches(cls):
        dir_path = Utils.get_ap89_collection_abs_path()
        file_paths = get_file_paths_to_parse(dir_path)
        logging.info("Total File to read: {}".format(len(file_paths)))
        return Utils.split_list_into_sub_lists(file_paths, no_of_sub_lists=8)

    @classmethod
    def add_documents_to_index(cls, documents, index_head=False, enable_stemming=False):
        tokenizer = Factory.create_tokenizer(Constants.CUSTOM_TOKENIZER_NAME)
        stopwords_filter = Factory.create_stopwords_filter(Constants.STOPWORDS_FILTER_NAME)
        stemmer = Factory.create_stemmer(Constants.SNOWBALL_STEMMER_NAME)
        compressor = Factory.create_compressor(Constants.GZIP_COMPRESSOR_NAME)
        # compressor = Factory.create_compressor(Constants.NO_OPS_COMPRESSOR_NAME)
        serializer = Factory.create_serializer(Constants.JSON_SERIALIZER_NAME)

        index = CustomIndex(tokenizer, stopwords_filter, stemmer, compressor, serializer)
        return index.add_documents(documents, index_head, enable_stemming)

    @classmethod
    @timing
    def main(cls):
        Utils.configure_logging()
        for file_path_batch in cls.create_files_to_read_batches():
            parsed_documents = get_parsed_documents(file_path_batch)
            results = Utils.run_task_parallelly(cls.add_documents_to_index, parsed_documents, 10)
            logging.info(results)


if __name__ == '__main__':
    HW2.main()
