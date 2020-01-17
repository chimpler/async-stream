import zlib

import zstd


def get_zstd_encoder():
    dctx = zstd.ZstdCompressor()
    return dctx.compressobj()

def get_zstd_decoder():
    dctx = zstd.ZstdDecompressor()
    return dctx.decompressobj()
