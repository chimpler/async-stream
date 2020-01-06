from typing import Callable

from asyncstream.readers import *
from asyncstream.writers import *

class AsyncioFileObj(object):
    def __init__(self, aiter: Optional[AsyncGenerator], compressor=None):
        self._aiter = aiter
        self._compressor = compressor

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await self.close()

    def __aiter__(self):
        return self

    def __anext__(self):
        return self._aiter.__anext__()

    async def write(self, buffer: str):
        result = self._compressor.compress(buffer)
        if result:
            await self._aiter.write(result)

    async def flush(self):
        result = self._compressor.flush()
        if result:
            await self._aiter.write(result)

    async def close(self):
        await self.flush()
        result = self._compressor.close()
        if result:
            await self._aiter.write(result)

def open(fd: AsyncGenerator, mode:str = 'rt', encoding: Optional[str] = None, compression: Optional[str] = None, buffer_size: int = 16384):
    if encoding:
        return AsyncioFileObj(
            (
                ','.join(row) + '\n'
                async for row in reader_columnar(fd, encoding, compression, buffer_size)
            )
        )
    else:
        if 'w' in mode:
            compressor = None
            if compression == 'gzip':
                compressor = GzipCompressor()
            elif compression == 'bzip2':
                compressor = Bzip2Compressor()
            elif compression == 'zstd':
                compressor = ZstdCompressor()
            elif compression is None:
                compressor = NoCompressor()

            return AsyncioFileObj(
                fd,
                compressor
            )
        else:
            return AsyncioFileObj(
                open_compressed(fd, compression, buffer_size)
            )
