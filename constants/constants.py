class Constants:
    PROJECT_ROOT = '/home/sumeet/PycharmProjects/CS6200-S20'
    DATA_DIR = 'data'
    AP_DATA_PATH = 'AP_DATA'
    AP89_COLLECTION = 'ap89_collection'
    AP_DATA_INDEX_NAME = 'ap_data'
    DOCUMENT_ID_MAPPING_FILE_NAME = 'document-id-mapping.json'
    AP_DATA_FILE_ENCODING = 'latin-1'
    PICKLE_PROTOCOL = 3

    # ES related configs
    CHUNK_SIZE = 10000
    TIMEOUT = 10

    BYES_TO_PROCESS_AT_ONCE_FOR_COMPRESSION = 8192
    NO_OF_PARALLEL_INDEXING_TASKS = 10
    TERMVECTOR_CACHE_SIZE = 10000

    # Compressor configs
    GZIP_COMPRESSOR_NAME = 'Gzip'
    NO_OPS_COMPRESSOR_NAME = 'NoOps'

    # Serializer configs
    JSON_SERIALIZER_NAME = 'Json'
    PICKLE_SERIALIZER_NAME = 'Pickle'
    TERMVECTOR_SERIALIZER_NAME = 'TermvectorSerializer'

    # Stemmer configs
    SNOWBALL_STEMMER_NAME = 'Snowball'

    # Stopwords filter configs
    STOPWORDS_FILTER_NAME = 'CustomStopwordsFilter'

    # Tokenizer configs
    CUSTOM_TOKENIZER_NAME = 'CustomTokenizer'
