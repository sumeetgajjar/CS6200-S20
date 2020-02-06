from nltk import SnowballStemmer

from HW_2.compressor import GzipCompressor, NoOpsCompressor, Compressor
from HW_2.indexer import CustomIndex
from HW_2.serializer import JsonSerializer, Serializer, PickleSerializer
from HW_2.stopwords import StopwordsFilter
from HW_2.tokenizer import Tokenzier
from constants.constants import Constants
from utils.utils import Utils


class Factory:

    @classmethod
    def create_compressor(cls, compressor_name: str) -> Compressor:
        if compressor_name == Constants.GZIP_COMPRESSOR_NAME:
            return GzipCompressor(Constants.BYES_TO_PROCESS_AT_ONCE_FOR_COMPRESSION)
        elif compressor_name == Constants.NO_OPS_COMPRESSOR_NAME:
            return NoOpsCompressor(Constants.AP_DATA_FILE_ENCODING)
        else:
            raise ValueError('Compressor not found')

    @classmethod
    def create_serializer(cls, serializer_name: str) -> Serializer:
        if serializer_name == Constants.JSON_SERIALIZER_NAME:
            return JsonSerializer()
        elif serializer_name == Constants.PICKLE_SERIALIZER_NAME:
            return PickleSerializer()
        else:
            raise ValueError('Serializer not found')

    @classmethod
    def create_stemmer(cls, stemmer_name):
        if stemmer_name == Constants.SNOWBALL_STEMMER_NAME:
            return SnowballStemmer('english')
        else:
            raise ValueError('Stemmer not found')

    @classmethod
    def create_stopwords_filter(cls, name):
        if name == Constants.STOPWORDS_FILTER_NAME:
            return StopwordsFilter(Utils.get_stopwords_file_path())
        else:
            raise ValueError('Stopwords filter not found')

    @classmethod
    def create_tokenizer(cls, tokenzier_name):
        if tokenzier_name == Constants.CUSTOM_TOKENIZER_NAME:
            return Tokenzier()
        else:
            raise ValueError('Tokenizer not found')

    @classmethod
    def create_custom_index(cls):
        tokenizer = cls.create_tokenizer(Constants.CUSTOM_TOKENIZER_NAME)
        stopwords_filter = cls.create_stopwords_filter(Constants.STOPWORDS_FILTER_NAME)
        stemmer = cls.create_stemmer(Constants.SNOWBALL_STEMMER_NAME)
        # compressor = cls.create_compressor(Constants.GZIP_COMPRESSOR_NAME)
        compressor = cls.create_compressor(Constants.NO_OPS_COMPRESSOR_NAME)
        serializer = cls.create_serializer(Constants.JSON_SERIALIZER_NAME)
        # serializer = cls.create_serializer(Constants.PICKLE_SERIALIZER_NAME)

        return CustomIndex(tokenizer, stopwords_filter, stemmer, compressor, serializer)
