try:
    from snappy import snappy
except ImportError as e:
    raise('Please install the snappy package: pip install snappy')


def get_snappy_encoder():
    return snappy.StreamCompressor()


def get_snappy_decoder():
    return snappy.StreamDecompressor()
