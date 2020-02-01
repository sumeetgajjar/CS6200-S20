from HW_1.main import get_file_paths_to_parse, get_parsed_documents
from utils.utils import Utils

if __name__ == '__main__':
    # text = "The car was in the car wash."
    # tokenizer = Tokenzier()
    # tokens = tokenizer.tokenize(text)
    # stopwords_filter = StopwordsFilter(Utils.get_stopwords_file_path())
    # stopwords_filter.filter(tokens)
    dir_path = Utils.get_ap89_collection_abs_path()
    file_paths = get_file_paths_to_parse(dir_path)
    parsed_documents = get_parsed_documents(file_paths)
