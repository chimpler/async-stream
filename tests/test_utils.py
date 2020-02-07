import bz2
import csv
import gzip
import io
from contextlib import contextmanager
from itertools import zip_longest
from typing import AsyncGenerator, Iterable, Union

import pandas as pd
import pyorc
import zstd
from snappy import snappy


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

def uncompress_snappy(filename: str):
    with open(filename, 'rb') as fd:
        dctx = snappy.StreamDecompressor()
        return dctx.decompress(fd.read()).splitlines(True)

@contextmanager
def open_file_or_bytesio(file: Union[str, io.BytesIO]):
    if isinstance(file, str):
        with open(file, 'rb') as fd:
            yield fd
    else:
        yield file


def compress_none(filename: str):
    with open(filename, 'rb') as fd:
        return fd.read()


def compress_gzip(file: Union[str, io.BytesIO]):
    buffer = io.BytesIO()
    with open_file_or_bytesio(file) as fd:
        with gzip.open(buffer, 'wb') as gzfd:
            gzfd.write(fd.read())
    return buffer.getvalue()


def compress_bzip2(file: Union[str, io.BytesIO]):
    buffer = io.BytesIO()
    with open_file_or_bytesio(file) as fd:
        with bz2.open(buffer, 'wb') as bz2fd:
            bz2fd.write(fd.read())
    return buffer.getvalue()


def compress_zstd(filename: str):
    with open(filename, 'rb') as fd:
        dctx = zstd.ZstdCompressor()
        return dctx.compress(fd.read())


def compress_snappy(file: Union[str, io.BytesIO]):
    with open_file_or_bytesio(file) as fd:
        comp = snappy.StreamCompressor()
        return comp.compress(fd.read())


def compress(file, compression=None):
    if compression is None:
        return compress_none(file)
    elif compression == 'gzip':
        return compress_gzip(file)
    elif compression == 'bzip2':
        return compress_bzip2(file)
    elif compression == 'zstd':
        return compress_zstd(file)
    elif compression == 'snappy':
        return compress_snappy(file)
    else:
        raise Exception('Compression {compression} not supported'.format(compression=compression))

def uncompress(file, compression=None):
    if compression is None:
        return uncompress_none(file)
    elif compression == 'gzip':
        return uncompress_gzip(file)
    elif compression == 'bzip2':
        return uncompress_bzip2(file)
    elif compression == 'zstd':
        return uncompress_zstd(file)
    elif compression == 'snappy':
        return uncompress_snappy(file)
    else:
        raise Exception('Compression {compression} not supported'.format(compression=compression))


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


def encode_orc(filename: str, compression: str = None, columns: Iterable[str] = None,
               column_types: Iterable[str] = None, skip_header=True):
    buffer = io.BytesIO()
    with open(filename, 'rt') as fd:
        reader = csv.reader(fd)

        struct = 'struct<{columns}>'.format(
            columns=','.join(
                name + ':' + (col_type if col_type else 'string')
                for name, col_type in zip_longest(columns, column_types)
            )
        )

        if skip_header:
            next(reader)

        if compression in (None, 'zlib', 'zstd'):
            compression_type = getattr(pyorc.CompressionKind, str(compression).upper())
        else:
            compression_type = pyorc.CompressionKind.NONE
        with pyorc.Writer(buffer, struct, compression=compression_type) as writer:
            for row in reader:
                writer.write(tuple(row))

        if compression in (None, 'zlib', 'zstd'):
            return buffer.getvalue()
        else:
            buffer.seek(0)
            return compress(buffer, compression)


def decode_orc(filename: str):
    with open(filename, "rb") as data:
        reader = pyorc.Reader(data)
        return '\n'.join(','.join([str(c) for c in row]) for row in reader)


def get_raw_rows(filename: str, compression=None):
    return list(csv.reader([line.decode('utf-8') for line in uncompress(filename, compression)]))


def get_raw_lines(filename: str, compression=None):
    return uncompress(filename, compression)
