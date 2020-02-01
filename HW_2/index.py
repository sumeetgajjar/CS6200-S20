class CustomIndex:
    def __init__(self, tokenizer, stopwords_filter, stemmer) -> None:
        self.tokenizer = tokenizer
        self.stopwords_filter = stopwords_filter
        self.stemmer = stemmer

    @classmethod
    def _calculate_and_update_tf_info(cls, document_id, tokens, tf_info):
        for token in tokens:
            term = token[0]
            if term not in tf_info:
                tf_info[term]['ttf'] = 0
                tf_info[term]['tf'] = {}

            term_tf_info = tf_info[term]['tf']
            if document_id not in term:
                term_tf_info[document_id] = {'tf': 0, 'pos': []}

            termvector = term_tf_info[document_id]
            termvector['tf'] += 1
            termvector['pos'].append(token[1])

    def _write_tf_info_to_index_file(self, tf_info):
        catalog = {}
        index_file_path = ''
        return catalog, index_file_path

    def _write_catalog_to_file(self, catalog):
        catalog_file_path = ''
        return catalog_file_path

    def add_documents(self, documents, index_head, enable_stemming):

        tf_info = {}
        for document in documents:
            tokens = self.tokenizer.tokenize(document['text'])

            if index_head:
                head_tokens = self.tokenizer.tokenize(document['head'])
                tokens.extend(head_tokens)

            tokens = self.stopwords_filter.filter(tokens)

            if enable_stemming:
                tokens = [self.stemmer.stem(token) for token in tokens]

            self._calculate_and_update_tf_info(document['id'], tokens, tf_info)

        catalog, index_file_path = self._write_tf_info_to_index_file(tf_info)
        catalog_file_path = self._write_catalog_to_file(catalog)
        return catalog_file_path, index_file_path
