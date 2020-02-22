from tempfile import SpooledTemporaryFile
from typing import Any, Iterable
from asyncstream import AsyncFileObj
from asyncstream.codecs import error_import_usage

try:
    import pandas as pd
except ImportError as e:
    error_import_usage('pandas')

try:
    import pyarrow
except ModuleNotFoundError:
    error_import_usage('pyarrow')



class ParquetDecompressor(object):
    def __init__(self, sep=b','):
        self._buffer = SpooledTemporaryFile(mode='wb')
        self._result = SpooledTemporaryFile(mode='wb')
        self._sep = sep
        self.eof = False

    def decompress(self, data: bytes):
        self._buffer.write(data)
        return b''

    def flush(self):
        self._buffer.seek(0)
        # TODO: to optimize it
        df = pd.read_parquet(self._buffer, engine='pyarrow')
        return df.to_csv(index=False).encode('utf-8')

    def __del__(self):
        self._buffer.close()
        self._result.close()


class ParquetCompressor(object):
    def __init__(self):
        self._buffer = None

    def compress(self, data: bytes):
        if self._buffer is None:
            self._buffer = SpooledTemporaryFile(mode='wb')
        self._buffer.write(data)

    def flush(self):
        if self._buffer:
            self._buffer.seek(0)
            df = pd.read_csv(self._buffer)
            with SpooledTemporaryFile(mode='wb') as wfd:
                df.to_parquet(wfd, engine='pyarrow')
                wfd.seek(0)
                return wfd.read()

    def __del__(self):
        if self._buffer:
            self._buffer.close()


# async def parquet_write(afd, , columns: Iterable[str] = None, column_types: Iterable[str] = None, compression = None, buffer_memory = 1024 * 1024):

async def parquet_write(afd, rows: Iterable[Iterable[Any]], columns: Iterable[str] = None,
                        column_types: Iterable[str] = None, compression=None, buffer_memory=1024 * 1024):
    with SpooledTemporaryFile(mode='w+b', max_size=buffer_memory) as wfd:
        df = pd.DataFrame(rows, columns=columns, dtype=column_types)
        df.to_parquet(wfd, engine='pyarrow', compression=compression)
        wfd.seek(0)


async def get_parquet_reader(afd: AsyncFileObj, buffer_memory=1024 * 1024):
    with SpooledTemporaryFile(mode='w+b', max_size=buffer_memory) as wfd:
        async for buf in afd:
            wfd.write(buf)

        wfd.flush()
        wfd.seek(0)
        table = pd.read_parquet(wfd, engine='pyarrow')

        for row in table.itertuples(index=False):
            yield row
