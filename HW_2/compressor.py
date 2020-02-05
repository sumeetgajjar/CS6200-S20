import gzip
from abc import abstractmethod
from io import BytesIO

from constants.constants import Constants


class Compressor:

    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    def compress_bytes(self, bytes_to_compress: bytes) -> bytes:
        pass

    @abstractmethod
    def decompress_bytes(self, bytes_to_decompress: bytes) -> bytes:
        pass


class GzipCompressor(Compressor):

    def __init__(self, bytes_to_read_at_once) -> None:
        self.bytes_to_read_at_once = bytes_to_read_at_once

    @property
    def name(self) -> str:
        return Constants.GZIP_COMPRESSOR_NAME

    def compress_bytes(self, bytes_to_compress: bytes) -> bytes:
        bio = BytesIO(bytes_to_compress)
        stream = BytesIO()
        compressor = gzip.GzipFile(fileobj=stream, mode='w')
        while True:  # until EOF
            chunk = bio.read(self.bytes_to_read_at_once)
            if not chunk:  # EOF?
                compressor.close()
                return stream.getvalue()
            compressor.write(chunk)

    def decompress_bytes(self, bytes_to_decompress: bytes) -> bytes:
        bio = BytesIO()
        stream = BytesIO(bytes_to_decompress)
        decompressor = gzip.GzipFile(fileobj=stream, mode='r')
        while True:  # until EOF
            chunk = decompressor.read(self.bytes_to_read_at_once)
            if not chunk:
                decompressor.close()
                bio.seek(0)
                return bio.read()
            bio.write(chunk)


class NoOpsCompressor(Compressor):

    def __init__(self, string_encoding) -> None:
        self.string_encoding = string_encoding

    @property
    def name(self) -> str:
        return Constants.NO_OPS_COMPRESSOR_NAME

    def compress_bytes(self, bytes_to_compress: bytes) -> bytes:
        return bytes_to_compress

    def decompress_bytes(self, bytes_to_decompress: bytes) -> bytes:
        return bytes_to_decompress
