class CustomIndex:
    def __init__(self, tokenizer, stopwords_filter, stemmer) -> None:
        self.tokenizer = tokenizer
        self.stopwords_filter = stopwords_filter
        self.stemmer = stemmer

    def add_documents(self, documents, index_head, enable_stemming):

        for document in documents:
            tokens = self.tokenizer.tokenize(document['text'])

            if index_head:
                head_tokens = self.tokenizer.tokenize(document['head'])
                tokens.extend(head_tokens)

            tokens = self.stopwords_filter.filter(tokens)

            if enable_stemming:
                tokens = [self.stemmer.stem(token) for token in tokens]
