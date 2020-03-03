import logging

from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient
from elasticsearch.helpers import bulk, scan

from constants.constants import Constants
from utils.decorators import timing

ES_HOSTS = [{"host": "localhost", "port": 9200}]
ES_CLIENT = None


class EsUtils:

    @classmethod
    def get_es_client(cls, timeout: int = Constants.ES_TIMEOUT):
        global ES_CLIENT
        if True:  # not ES_CLIENT:
            logging.debug("Creating the ES client")
            ES_CLIENT = Elasticsearch(hosts=ES_HOSTS, send_get_body_as='POST', timeout=timeout)
            logging.debug("ES client created")

        return ES_CLIENT

    @classmethod
    def get_indices_client(cls):
        logging.debug("Creating ES Indices Client")
        client = IndicesClient(cls.get_es_client())
        logging.debug("Indices Client created")
        return client

    @classmethod
    def create_es_index(cls, name, index_config):
        logging.info('Creating "{}" index'.format(name))
        es = cls.get_es_client()
        response = es.indices.create(index=name, body=index_config)
        logging.info(response)
        logging.info('"{}" index created'.format(name))

    @classmethod
    def delete_es_index(cls, name, ignore_unavailable):
        logging.info('Deleting "{}" index'.format(name))
        es_client = cls.get_es_client()
        response = es_client.indices.delete(index=name, ignore_unavailable=ignore_unavailable)
        logging.info(response)
        logging.info('"{}" index deleted'.format(name))

    @classmethod
    @timing
    def bulk_add_document_to_ap_data_index(cls, documents: list, chunk_size: int = Constants.CHUNK_SIZE):
        def get_document_generator():
            for document in documents:
                yield {
                    '_index': Constants.AP_DATA_INDEX_NAME,
                    '_id': document['id'],
                    '_source': {
                        'text': document['text'],
                        'length': document['length']
                    }
                }

        es_client = cls.get_es_client()
        response = bulk(client=es_client, actions=get_document_generator(), chunk_size=chunk_size)
        logging.info(response)

    @classmethod
    def get_match_all_query(cls):
        return {
            "query": {
                "match_all": {}
            },
            "stored_fields": []
        }

    @classmethod
    @timing
    def get_all_document_ids(cls, index_name: str, chunk_size: int = Constants.CHUNK_SIZE) -> list:
        es_client = cls.get_es_client()
        response = scan(es_client, query=cls.get_match_all_query(), index=index_name, size=chunk_size)
        document_ids = [result['_id'] for result in response]
        logging.info("{} total docs in index: {}".format(len(document_ids), index_name))
        return document_ids

    @classmethod
    def get_mtermvector_query(cls, document_ids: list):
        return {
            "ids": document_ids,
            "parameters": {
                "fields": ["text"],
                "term_statistics": True,
                "positions": False,
                "offsets": False,
                "field_statistics": False,
                "payloads": False
            }
        }

    @classmethod
    @timing
    def get_termvectors(cls, index_name: str, document_ids: list, timeout: int = Constants.ES_TIMEOUT) -> dict:
        es_client = cls.get_es_client(timeout)
        response = es_client.mtermvectors(index=index_name, body=cls.get_mtermvector_query(document_ids))
        return response['docs']

    @classmethod
    def get_average_doc_length(cls, index_name: str):
        es_client = cls.get_es_client()
        response = es_client.search(index=index_name, body={
            "aggs": {
                "avg_doc_length": {
                    "avg": {
                        "field": "length"
                    }
                }
            },
            "stored_fields": [],
            "size": 0
        })
        return response['aggregations']['avg_doc_length']['value']

    @classmethod
    def get_vocabulary_size(cls, index_name: str):
        es_client = cls.get_es_client()
        response = es_client.search(index=index_name, body={
            "aggs": {
                "vocab_size": {
                    "cardinality": {
                        "field": "text"
                    }
                }
            },
            "size": 0
        })
        return response['aggregations']['vocab_size']['value']

    @classmethod
    def get_significant_terms(cls, index_name: str, term: str):
        es_client = cls.get_es_client()
        response = es_client.search(index=index_name, body={
            "query": {
                "terms": {"text": [term]}
            },
            "aggregations": {
                "significant_text_types": {
                    "significant_terms": {
                        "field": "text",
                        "gnd": {}
                    }
                }
            },
            "size": 0
        })
        return response['aggregations']['significant_text_types']['buckets']
