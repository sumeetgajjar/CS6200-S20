import logging

from HW_1.main import get_file_paths_to_parse, get_parsed_documents
from HW_2.factory import Factory
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
    def add_documents_to_index(cls, index_head, enable_stemming):
        dir_path = Utils.get_ap89_collection_abs_path()
        file_paths = get_file_paths_to_parse(dir_path)
        logging.info("Total File to read: {}".format(len(file_paths)))
        parsed_documents = get_parsed_documents(file_paths)

        custom_index = Factory.create_custom_index()
        metadata = custom_index.index_documents(parsed_documents, index_head, enable_stemming)
        return custom_index

    @classmethod
    @timing
    def main(cls):
        Utils.configure_logging()
        custom_index = cls.add_documents_to_index(False, True)


if __name__ == '__main__':
    HW2.main()
    # tokens = Factory.create_tokenizer(Constants.CUSTOM_TOKENIZER_NAME).tokenize("The car was in the car wash.")
    # print(tokens)
    # filtered_tokens = Factory.create_stopwords_filter(Constants.STOPWORDS_FILTER_NAME).filter(tokens)
    # print(filtered_tokens)
