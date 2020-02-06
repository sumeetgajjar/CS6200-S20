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
        # custom_index = cls.add_documents_to_index(False, True)

        custom_index = Factory.create_custom_index()
        custom_index.init_index(
            '/home/sumeet/PycharmProjects/CS6200-S20/data/custom-index/metadata/02-05-2020-21:15:16-112b1670-7d6b-4f8c-b9ee-06509b8b0e1a.txt')
        alexand = 'alexand'
        print(custom_index.get_termvector('%s' % alexand))
        print(len(custom_index.get_termvector(alexand).tfInfo))
        print(custom_index.get_termvector(alexand).ttf)


if __name__ == '__main__':
    HW2.main()
    # tokens = Factory.create_tokenizer(Constants.CUSTOM_TOKENIZER_NAME).tokenize("The car was in the car wash.")
    # print(tokens)
    # filtered_tokens = Factory.create_stopwords_filter(Constants.STOPWORDS_FILTER_NAME).filter(tokens)
    # print(filtered_tokens)
