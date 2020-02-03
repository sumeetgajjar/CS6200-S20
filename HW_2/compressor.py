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
    def compress_string_to_bytes(self, string_to_compress: str) -> bytes:
        pass

    @abstractmethod
    def decompress_bytes_to_string(self, bytes_to_decompress: bytes) -> str:
        pass


class GzipCompressor(Compressor):

    def __init__(self, bytes_to_read_at_once) -> None:
        self.bytes_to_read_at_once = bytes_to_read_at_once

    @property
    def name(self) -> str:
        return Constants.GZIP_COMPRESSOR_NAME

    def compress_string_to_bytes(self, string_to_compress: str) -> bytes:
        bio = BytesIO()
        bio.write(string_to_compress.encode("utf-8"))
        bio.seek(0)
        stream = BytesIO()
        compressor = gzip.GzipFile(fileobj=stream, mode='w')
        while True:  # until EOF
            chunk = bio.read(self.bytes_to_read_at_once)
            if not chunk:  # EOF?
                compressor.close()
                return stream.getvalue()
            compressor.write(chunk)

    def decompress_bytes_to_string(self, bytes_to_decompress: bytes) -> str:
        bio = BytesIO()
        stream = BytesIO(bytes_to_decompress)
        decompressor = gzip.GzipFile(fileobj=stream, mode='r')
        while True:  # until EOF
            chunk = decompressor.read(self.bytes_to_read_at_once)
            if not chunk:
                decompressor.close()
                bio.seek(0)
                return bio.read().decode("utf-8")
            bio.write(chunk)


class NoOpsCompressor(Compressor):

    def __init__(self, string_encoding) -> None:
        self.string_encoding = string_encoding

    @property
    def name(self) -> str:
        return Constants.NO_OPS_COMPRESSOR_NAME

    def compress_string_to_bytes(self, string_to_compress: str) -> bytes:
        return bytearray(string_to_compress, encoding=self.string_encoding)

    def decompress_bytes_to_string(self, bytes_to_decompress: bytes) -> str:
        return bytes_to_decompress.decode(self.string_encoding)
