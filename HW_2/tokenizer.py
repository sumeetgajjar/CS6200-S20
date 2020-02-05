import re


class Tokenzier:
    def __init__(self) -> None:
        self.split_regex = re.compile("([\\d.]+|\\w+)")

    def tokenize(self, document: str) -> list:
        tokens = re.findall(self.split_regex, document)

        i = 1
        result = []
        for token in tokens:
            token = token.lower()
            result.append((token, i))
            i += 1

        return result
