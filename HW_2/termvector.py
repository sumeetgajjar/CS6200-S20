class TfInfo:

    def __init__(self) -> None:
        self.tf: int = 0
        self.positions: list = []


class TermVector:

    def __init__(self, term) -> None:
        self.term: str = term
        self.ttf: int = 0
        self.tfInfo: dict = {}
