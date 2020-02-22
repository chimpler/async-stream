import io
from tempfile import SpooledTemporaryFile
from typing import Any, Iterable

from asyncstream.codecs import error_import_usage

try:
    import pandas as pd
except ModuleNotFoundError:
    error_import_usage('pandas')

try:
    import pyorc
except ModuleNotFoundError:
    error_import_usage('pyorc')

from asyncstream import AsyncFileObj


class OrcDecompressor(object):
    def __init__(self):
        # self._buffer = TemporaryFile(mode='wb')
        # TODO try to make SpooledTemporaryFile work
        self._buffer = io.BytesIO()
        self._result = SpooledTemporaryFile(mode='wt')

    def decompress(self, data: bytes):
        self._buffer.write(data)
        return b''

    def flush(self):
        self._buffer.seek(0)
        # TODO: to optimize it
        reader = pyorc.Reader(self._buffer)
        columns = reader.schema.fields.keys()
        return (','.join(columns) + '\n' + '\n'.join(
            ','.join(
                [str(c) for c in row]
            )
            for row in reader
        )).encode('utf-8') + b'\n'

    def __del__(self):
        self._buffer.close()
        self._result.close()


class OrcCompressor(object):
    def __init__(self):
        self._buffer = SpooledTemporaryFile(mode='wb')

    def compress(self, data: bytes):
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
            # [col.decode('utf-8') if isinstance(col, bytes) else str(col) for col in row]
