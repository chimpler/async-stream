try:
    import zstd
except ImportError as e:
    raise('Please install the zstandard package: pip install zstandard')


def get_zstd_encoder():
    dctx = zstd.ZstdCompressor()
    return dctx.compressobj()

def get_zstd_decoder():
    dctx = zstd.ZstdDecompressor()
    return dctx.decompressobj()
