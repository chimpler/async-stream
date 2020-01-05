import asyncio
import bz2
import csv
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
