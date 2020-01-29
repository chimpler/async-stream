import asyncio
import gzip

import aiofiles
from asyncstream.async_file_obj import AsyncFileObj
from asyncstream.async_reader import AsyncReader
from asyncstream.codecs.bzip2_codec import get_bzip2_encoder, get_bzip2_decoder
from asyncstream.codecs.gzip_codec import get_gzip_encoder, get_gzip_decoder
from asyncstream.codecs.none_codec import NoneDecompressor, NoneCompressor
from asyncstream.codecs.orc_codec import OrcCompressor, OrcDecompressor
from asyncstream.codecs.parquet_codec import ParquetCompressor, ParquetDecompressor
from asyncstream.codecs.snappy_codec import get_snappy_encoder, get_snappy_decoder
from asyncstream.codecs.zstd_codec import get_zstd_encoder, get_zstd_decoder

def open(afd, encoding=None, compression=None, compress_level=-1):
    if encoding is None and compression is None:
        compressor = NoneCompressor()
        decompressor = NoneDecompressor()
    elif encoding == 'parquet':
        compressor = ParquetCompressor()
        decompressor = ParquetDecompressor()
    elif encoding == 'orc':
        compressor = OrcCompressor()
        decompressor = OrcDecompressor()
    elif compression == 'gzip':
        compressor = get_gzip_encoder()
        decompressor = get_gzip_decoder()
    elif compression == 'bzip2':
        compressor = get_bzip2_encoder()
        decompressor = get_bzip2_decoder()
    elif compression == 'zstd':
        compressor = get_zstd_encoder()
        decompressor = get_zstd_decoder()
    elif compression == 'snappy':
        compressor = get_snappy_encoder()
        decompressor = get_snappy_decoder()
    else:
        raise ValueError('Unsupported compression %s' % compression)

    return AsyncFileObj(afd, compressor, decompressor)

def reader(afd: AsyncFileObj, has_header: bool = True):
    return AsyncReader(afd, has_header=has_header)

async def run():
    # with gzip.open('/Users/fdang/tmp/beacon_data/beacons/0000_part_00.gz', 'rb') as fd:
    #     for line in fd:
    #         pass
    #         print(line)
    # exit(1)
    # async with aiofiles.open('test.txt.gz', 'rb') as afd:
    # async with aiofiles.open('/Users/fdang/tmp/beacon_data/beacons/0000_part_00.gz', 'rb') as afd:
    async with aiofiles.open('/Users/fdang/tmp/parquet_beacon/0025_part_00.parquet', 'rb') as afd:
    # async with aiofiles.open('/Users/fdang/tmp/orc/part-00001-ffe952de-a465-46a4-bfc1-c989b3b58cc9-c000.snappy.orc', 'rb') as afd:
        # async with open(afd, None, 'gzip') as fd:
        # async with aiofiles.open('/tmp/aa.gz', 'wb') as wfd:
        async with open('/tmp/aa.gz', compression='gzip') as gzfd:
                async with open(afd, 'parquet', None) as fd:
                    async with reader(fd) as r:
                        async for row in r:
                            await gzfd.write(b','.join(row) + b'\n')

if __name__ == '__main__':
    print('====')
    asyncio.run(run())
    print('====')

# from typing import Callable
#
# from asyncstream.readers import *
# from asyncstream.writers import *
#
#
# class AsyncioFileObj(object):
#     def __init__(self, aiter: Optional[AsyncGenerator], compressor=None):
#         self._aiter = aiter
#         self._compressor = compressor
#
#     async def __aenter__(self):
#         return self
#
#     async def __aexit__(self, *exc):
#         await self.close()
#
#     def __aiter__(self):
#         return self
#
#     def __anext__(self):
#         return self._aiter.__anext__()
#
#     async def write(self, buffer: str):
#         result = self._compressor.compress(buffer)
#         if result:
#             await self._aiter.write(result)
#
#     async def flush(self):
#         if self._compressor:
#             result = self._compressor.flush()
#             if result:
#                 await self._aiter.write(result)
#
#     async def close(self):
#         await self.flush()
#         if self._compressor:
#             result = self._compressor.close()
#             if result:
#                 await self._aiter.write(result)
#
#
# def open(fd: AsyncGenerator, mode: str = 'rt', encoding: Optional[str] = None, compression: Optional[str] = None,
#          buffer_size: int = 16384):
#     if encoding:
#         if encoding == 'parquet':
#             reader = reader_parquet(fd, encoding, compression, buffer_size)
#         elif encoding == 'orc':
#             reader = reader_orc(fd, encoding, compression, buffer_size)
#         return AsyncioFileObj(
#             (
#                 ','.join(row) + '\n'
#                 async for row in reader
#             )
#         )
#     else:
#         if 'w' in mode:
#             compressor = None
#             if compression == 'gzip':
#                 compressor = GzipCompressor()
#             elif compression == 'bzip2':
#                 compressor = Bzip2Compressor()
#             elif compression == 'zstd':
#                 compressor = ZstdCompressor()
#             elif compression == 'snappy':
#                 compressor = snappy.StreamDecompressor()
#             elif compression is None:
#                 compressor = NoCompressor()
#
#             return AsyncioFileObj(
#                 fd,
#                 compressor
#             )
#         else:
#             return AsyncioFileObj(
#                 open_compressed(fd, compression, buffer_size)
#             )
