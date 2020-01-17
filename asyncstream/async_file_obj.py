from typing import Optional


class AsyncFileObj(object):
    def __init__(self, afd, compressor, decompressor, ignore_header=False, buffer_size=1024):
        self._afd = afd
        self._compressor = compressor
        self._decompressor = decompressor
        self._ignore_header = ignore_header
        self._header = None
        self._buffer = b''
        self._buffer_size = buffer_size

    async def read(self, n: Optional[int] = None):
        data = await self._afd.read(n)
        if data:
            return self._decompressor.decompress(data)
        else:
            return b''

    async def write(self, buffer: bytes):
        compressed_data = self._compressor(buffer)
        if compressed_data:
            return await self._afd.write(compressed_data)
        else:
            return 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        while True:
            tokens = self._buffer.split(b'\n', 1)
            if len(tokens) == 2:
                self._buffer = tokens[1]
                return tokens[0]
            else:
                tmp = await self.read(self._buffer_size)
                if tmp:
                    self._buffer += tmp
                    continue
                else:
                    if self._buffer:
                        return self._buffer
                    else:
                        raise StopAsyncIteration

    async def flush(self):
        compressed_data = self._compressor.flush()
        if compressed_data:
            return await self._afd.write(compressed_data)
        else:
            return 0

    async def close(self):
        self._compressor.flush()
        # self._compressor.close()
        # self._decompressor.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        return await self.close()