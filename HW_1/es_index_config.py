class EsIndexConfig:

    @classmethod
    def get_ap_data_index_config(cls):
        return {
            "settings": {
                "number_of_shards": 8,
                "number_of_replicas": 1,
                "analysis": {
                    "filter": {
                        "english_stop": {
                            "type": "stop",
                            "stopwords_path": "my_stoplist.txt"
                        },
                        "snowball_stemmer": {
                            "type": "snowball",
                            "language": "English"
                        }
                    },
                    "analyzer": {
                        "stopped": {
                            "type": "custom",
                            "tokenizer": "standard",
                            "filter": [
                                "lowercase",
                                "english_stop",
                                "snowball_stemmer"
                            ]
                        }
                    }
                }
            },
            "mappings": {
                "properties": {
                    "text": {
                        "type": "text",
                        "fielddata": True,
                        "analyzer": "stopped",
                        "index_options": "positions"
                    },
                    "length": {
                        "type": "integer",
                        "index": False
                    }
                }
            }
        }
