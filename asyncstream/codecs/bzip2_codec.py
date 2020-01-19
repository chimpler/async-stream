import bz2


def get_bzip2_encoder():
    return bz2.BZ2Compressor()


def get_bzip2_decoder():
    return bz2.BZ2Decompressor()
