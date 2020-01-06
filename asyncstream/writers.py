import asyncio
import bz2
import csv
import struct
import zlib
from functools import partial
from io import FileIO
from tempfile import SpooledTemporaryFile
from typing import AsyncGenerator, Optional
import pyarrow.parquet as pq
import pyarrow.orc as orc
import aiofiles
from pandas import DataFrame
from pyarrow.lib import Table

from asyncstream.readers import open_parquet, open_parquet_pandas


class GzipCompressor(object):
    GZIP_HEADER = b'\x1f\x8b\x08\x00\x00\x00\x00\x00\x02\xff'

    def __init__(self):
        self._comp = zlib.compressobj(9, zlib.DEFLATED, -zlib.MAX_WBITS, zlib.DEF_MEM_LEVEL, 0)
        self._crc = zlib.crc32(b"") & 0xffffffff
        self._size = 0
        self._before_header = True

    def compress(self, data: str):
        if self._before_header:
            self._before_header = False
            return self.GZIP_HEADER
        else:
            self._crc = zlib.crc32(data, self._crc) & 0xffffffff
            self._size += len(data)
            return self._comp.compress(data)

    def flush(self):
        return self._comp.flush()

    def close(self):
        return struct.pack("<2L", self._crc, self._size & 0xffffffff)


# async def block_gzip(fd: FileIO, buffer: str):
#     comp = zlib.compressobj(zlib.MAX_WBITS | 16)
#     data = comp.compress(buffer)
#     if data is None:
#         data = comp.flush()
#     await fd.write(data)

# async def open_zstd_block(fd: FileIO):
#     cctx = zstd.ZstdCompressor()
#     cobj = cctx.compressobj()
#
#     async for buf in fd:


async def write_pandas_parquet(df: DataFrame, wfd: FileIO, compression: Optional[str] = None,
                               buffer_memory: int = 1048576):
    pq.write_table(df, compression=compression, where=wfd)


async def write_pandas_orc(df: DataFrame, wfd: FileIO, compression: Optional[str] = None, buffer_memory: int = 1048576):
    raise Exception('Not implemented yet!')

# async def run():
#     with open('/tmp/abc.orc', 'wb') as wfd:
#         t = pq.read_table('samples/volumes/localstack/s3/bucket/test/nation.dict.parquet')
#         async with aiofiles.open('samples/volumes/localstack/s3/bucket/test/nation.dict.parquet', 'rb') as fd:
#             table = await open_parquet_pandas(fd)
#             table = Table.from_pandas(table.to_pandas())
#             await write_pandas_orc(table, wfd)
#
#
# asyncio.run(run())
