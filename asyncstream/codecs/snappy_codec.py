import zlib

import zstd
from snappy import snappy


def get_snappy_encoder():
    return snappy.StreamCompressor()


def get_snappy_decoder():
    return snappy.StreamDecompressor()
