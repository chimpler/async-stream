class NoneDecompressor(object):
    def decompress(self, data: bytes) -> bytes:
        return data

    def flush(self) -> bytes:
        return b''

class NoneCompressor(object):
    def compress(self, data: bytes) -> bytes:
        return data

    def flush(self) -> bytes:
        return b''
