import bz2
import csv
import gzip
import io
from io import FileIO
from tempfile import SpooledTemporaryFile
from typing import AsyncGenerator

import pandas as pd
import zstd


async def async_gen_to_list(async_gen: AsyncGenerator):
    result = []
    async for obj in async_gen:
        result.append(obj)

    return result


def uncompress_none(filename: str):
    with open(filename, 'rb') as fd:
        return list(fd)


def uncompress_gzip(filename: str):
    with gzip.open(filename) as gzfd:
        return list(gzfd)


def uncompress_bzip2(filename: str):
    with bz2.open(filename) as bz2fd:
        return list(bz2fd)


def uncompress_zstd(filename: str):
    with open(filename, 'rb') as fd:
        dctx = zstd.ZstdDecompressor()
        return dctx.decompress(fd.read(), max_output_size=1048576).splitlines(True)


def compress_none(filename: str):
    with open(filename, 'rb') as fd:
        return fd.read()


def compress_gzip(filename: str):
    buffer = io.BytesIO()
    with open(filename, 'rb') as fd:
        with gzip.open(buffer, 'wb') as gzfd:
            gzfd.write(fd.read())
    return buffer.getvalue()


def compress_bzip2(filename: str):
    buffer = io.BytesIO()
    with open(filename, 'rb') as fd:
        with bz2.open(buffer, 'wb') as bz2fd:
            bz2fd.write(fd.read())
    return buffer.getvalue()


def compress_zstd(filename: str):
    with open(filename, 'rb') as fd:
        dctx = zstd.ZstdCompressor()
        return dctx.compress(fd.read())


def encode_parquet(filename: str, compression: str = None):
    with open(filename, 'rt') as fd:
        reader = csv.reader(fd)
        header = next(reader)
        df = pd.DataFrame(list(reader), columns=header)
        buffer = io.BytesIO()
        df.to_parquet(buffer, compression=compression)
        return buffer.getvalue()


def decode_parquet(filename: str):
    with open(filename, 'rb') as fd:
        df = pd.read_parquet(fd, engine='pyarrow')
        return df.to_csv(index=False).rstrip('\n')
