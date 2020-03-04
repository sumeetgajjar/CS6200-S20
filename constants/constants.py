from sqlalchemy import create_engine


class Constants:
    PROJECT_ROOT = '/home/sumeet/PycharmProjects/CS6200-S20'
    DATA_DIR = 'data'
    AP_DATA_PATH = 'AP_DATA'
    AP89_COLLECTION = 'ap89_collection'
    AP_DATA_INDEX_NAME = 'ap_data'
    CRAWLED_DATA_INDEX_NAME = 'crawled_data'
    CRAWLED_RESPONSE_DIR = 'crawled_response'
    USER_AGENT_FILE_NAME = 'user-agents.txt'
    DOCUMENT_ID_MAPPING_FILE_NAME = 'document-id-mapping.json'
    LINK_GRAPH_CSV_FILE_NAME = 'link-graph.csv'
    AP_DATA_FILE_ENCODING = 'latin-1'
    PICKLE_PROTOCOL = 3

    # ES related configs
    CHUNK_SIZE = 10000
    ES_TIMEOUT = 10

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
    MYSQL_ENGINE = create_engine('mysql+mysqldb://cs6200:cs6200@127.0.0.1:3306/cs6200?autocommit=true')

    # Redis Config
    REDIS_HOST = '127.0.0.1'
    REDIS_PORT = 6379
    MYSQL_POOL_NAME = 'cs6200'
    REDIS_SOCKET_TIMEOUT = 10
    REDIS_SEPARATOR = "::"

    TOPIC_KEYWORDS = {'1521', '1776', 'allies', 'America', 'america', 'american', 'Britain', 'colonies', 'congress',
                      'declaration', 'father', 'forces', 'founding', 'france', 'George', 'history', 'independence',
                      'july', 'revolution', 'revolutionary', 'states', 'thirteen', 'united', 'war', 'Washington'}

    # CRAWLING configs
    MAX_URLS_TO_CRAWL = 50000
    MAX_URLS_TO_CRAWL_KEY = 'MAX_URLS_TO_CRAWL'
    TOTAL_URL_CRAWLED_KEY = 'TOTAL_URLS_CRAWLED'
    DEFAULT_CRAWL_DELAY = 1
    TAGS_TO_REMOVE = ['img', 'iframe', 'script', 'stylesheet', 'map', 'progress', 'video', 'audio', 'area', 'embed']
    ROBOTS_TXT_CACHE_SIZE = 2000
    HTML_PARSER = 'lxml'
    # connection timeout and read timeout
    CRAWLER_TIMEOUT = (3, 20)
    CRAWLER_RETRY = 2

    # Crawled Urls Bloom Filter configs
    CRAWLED_URLS_BF = 'CRAWLED_URLS_BF'
    CRAWLED_URLS_BF_ERROR_RATE = 0.01
    CRAWLED_URLS_BF_CAPACITY = MAX_URLS_TO_CRAWL * 2

    # Url Processor configs
    URL_PROCESSOR_BATCH_SIZE_KEY = 'URL_PROCESSOR_BATCH_SIZE'
    URL_PROCESSOR_QUEUE_NAME_TEMPLATE = 'QUEUES::URL::PROCESSOR::{}'
    URL_PROCESSOR_DEFAULT_BATCH_SIZE = 10
    NO_OF_THREADS_PER_URL_PROCESSOR = 10
    NO_OF_URL_PROCESSORS = 10
    URL_PROCESSOR_SLEEP_TIME = 10  # seconds
    TIME_FORMAT = "%d-%m-%Y-%H:%M:%S.%f"
    LINK_GRAPH_INSERT_BATCH_SIZE = 2500

    # Url Mapper configs
    URL_MAPPER_QUEUE_TO_REDIS_RETRY = 3
    URL_MAPPER_SLEEP_TIME = 1  # seconds

    # Robots Txt
    ROBOTS_TXT_FILE_NAME = "robots.txt"

    # Frontier Manager
    FRONTIER_MANAGER_REDIS_QUEUE = "QUEUES::FRONTIER"
    DOMAIN_INLINKS_COUNT_KEY = "DOMAIN_INLINKS_COUNT"
    URL_INLINKS_COUNT_KEY = "URL_INLINKS_COUNT"
    RATE_LIMITED_URL_WEIGHT = 100000
    DOMAIN_RELEVANCE_KEY = 'DOMAIN_RELEVANCE'
    URL_RELEVANCE_KEY = 'URL_RELEVANCE'
    URLS_TO_CONSIDER_BASED_ON_SCORES = 100
