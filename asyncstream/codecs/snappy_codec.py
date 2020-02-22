from asyncstream.codecs import error_import_usage

try:
    from snappy import snappy
except ImportError as e:
    error_import_usage('snappy')


def get_snappy_encoder():
    return snappy.StreamCompressor()


def get_snappy_decoder():
    return snappy.StreamDecompressor()
