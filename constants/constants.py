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

    # MySQL config
    MYSQL_HOST = '127.0.0.1'
    MYSQL_PORT = 3306
    MYSQL_DATABASE = 'cs6200'
    MYSQL_USERNAME = 'cs6200'
    MYSQL_PASSWORD = 'cs6200'

    # Redis Config
    REDIS_HOST = '127.0.0.1'
    REDIS_PORT = 6379
    MYSQL_POOL_NAME = 'cs6200'
    REDIS_SOCKET_TIMEOUT = 10
    REDIS_SEPARATOR = "::"

    TOPIC_KEYWORDS = {'1521', 'AMERICAN', 'INDEPENDENCE', 'WAR'}

    # CRAWLING configs
    MAX_URLS_TO_CRAWL = 60000
    DEFAULT_CRAWL_DELAY = 1
    TAGS_TO_REMOVE = ['img', 'iframe', 'script', 'stylesheet', 'map', 'progress', 'video', 'audio', 'area', 'embed']
    ROBOTS_TXT_CACHE_SIZE = 2000
    HTML_PARSER = 'lxml'
    # connection timeout and read timeout
    CRAWLER_TIMEOUT = (3, 20)

    # Url Processor configs
    URLS_BATCH_SIZE_KEY = 'URLS_BATCH_SIZE'
    URLS_BATCH_SIZE = 20
    NO_OF_THREADS_PER_URL_PROCESSOR = 10
    NO_OF_URL_PROCESSORS = 10
    URL_PROCESSOR_SLEEP_TIME = 10  # seconds
