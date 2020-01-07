import bz2
import gzip
from io import FileIO
from typing import AsyncGenerator

import zstandard
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
