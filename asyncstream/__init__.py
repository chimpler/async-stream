import asyncio
from typing import Optional, Iterable, Union

import aiofiles
from aiofiles.threadpool import AsyncBufferedIOBase

from asyncstream.async_file_obj import AsyncFileObj
from asyncstream.async_reader import AsyncReader
from asyncstream.async_writer import AsyncWriter

def open(afd: Union[str, AsyncBufferedIOBase], mode=None, encoding=None, compression=None, compress_level=-1):
    if encoding is None and compression is None:
        from asyncstream.codecs.none_codec import NoneDecompressor, NoneCompressor
        compressor = NoneCompressor()
        decompressor = NoneDecompressor()
    elif encoding == 'parquet':
        from asyncstream.codecs.parquet_codec import ParquetCompressor, ParquetDecompressor
        compressor = ParquetCompressor()
        decompressor = ParquetDecompressor()
    elif encoding == 'orc':
        from asyncstream.codecs.orc_codec import OrcCompressor, OrcDecompressor
        compressor = OrcCompressor()
        decompressor = OrcDecompressor()
    elif compression == 'gzip':
        from asyncstream.codecs.gzip_codec import get_gzip_encoder, get_gzip_decoder
        compressor = get_gzip_encoder()
        decompressor = get_gzip_decoder()
    elif compression == 'bzip2':
        from asyncstream.codecs.bzip2_codec import get_bzip2_encoder, get_bzip2_decoder
        compressor = get_bzip2_encoder()
        decompressor = get_bzip2_decoder()
    elif compression == 'zstd':
        from asyncstream.codecs.zstd_codec import get_zstd_encoder, get_zstd_decoder
        compressor = get_zstd_encoder()
        decompressor = get_zstd_decoder()
    elif compression == 'snappy':
        from asyncstream.codecs.snappy_codec import get_snappy_encoder, get_snappy_decoder
        compressor = get_snappy_encoder()
        decompressor = get_snappy_decoder()
    else:
        raise ValueError('Unsupported compression %s' % compression)

    return AsyncFileObj(afd, mode, compressor, decompressor)


def reader(afd: AsyncFileObj, has_header: bool = True, columns: Optional[Iterable[str]] = None,
           column_types: Optional[Iterable[str]] = None, delimiter=','):
    return AsyncReader(afd, columns, column_types, has_header)


def writer(afd: AsyncFileObj, has_header: bool = True, columns: Optional[Iterable[str]] = None,
           column_types: Optional[Iterable[str]] = None):
    return AsyncWriter(afd, has_header=has_header)
