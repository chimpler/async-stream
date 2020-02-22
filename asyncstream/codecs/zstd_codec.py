from asyncstream.codecs import error_import_usage

try:
    import zstd
except ImportError as e:
    error_import_usage('zstd')


def get_zstd_encoder():
    dctx = zstd.ZstdCompressor()
    return dctx.compressobj()

def get_zstd_decoder():
    dctx = zstd.ZstdDecompressor()
    return dctx.decompressobj()
