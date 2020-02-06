from constants.constants import Constants


class StopwordsFilter:
    def __init__(self, stopwords_file_path: str) -> None:
        self.stop_word_file_path = stopwords_file_path
        self.stop_words = self._read_stop_words_from_file()

    def _read_stop_words_from_file(self) -> set:
        stopwords = set()
        with open(self.stop_word_file_path) as file:
            for line in file:
                stopwords.add(line.lower().strip())

        return stopwords

    @property
    def name(self):
        return Constants.STOPWORDS_FILTER_NAME

    def filter(self, tokens):
        filtered_tokens = []
        i = 1
        for token in tokens:
            if token[0] not in self.stop_words:
                filtered_tokens.append((token[0], i))
                i += 1

        return filtered_tokens
