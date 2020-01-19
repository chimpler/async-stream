from typing import Optional

import aiofiles

class AsyncFileObj(object):
    def __init__(self, afd, compressor, decompressor, ignore_header=False, buffer_size=1024 * 1024):
        self._afd = afd
        self._compressor = compressor
        self._decompressor = decompressor
        self._ignore_header = ignore_header
        self._header = None
        self._buffer = b''
        self._buffer_size = buffer_size
        self._eof = False
        self._lines = []
        self._index = 0

    async def read(self, n: Optional[int] = None):
        buffer_size = n if n else 1024 * 1024
        while True:
            if self._eof or len(self._buffer) >= buffer_size:
                result = self._buffer[:buffer_size]
                self._buffer = self._buffer[buffer_size:]
                return result

            data = await self._afd.read(n)
            if data:
                self._buffer += self._decompressor.decompress(data)
            else:
                self._buffer += self._decompressor.flush()
                self._eof = True

        # data = await self._afd.read(n)
        # if data:
        #     return (
        #
        #         self._decompressor.decompress(data)
        #     )
        # else:
        #     return b''

    async def write(self, buffer: bytes):
        compressed_data = self._compressor.compress(buffer)
        if compressed_data:
            return await self._afd.write(compressed_data)
        else:
            return 0

    # async def __aiter__(self):
    #     print('wooof')
    #     if isinstance(self._afd, str):
    #         async with aiofiles.open(self._afd, 'wb') as fd:
    #             self._afd = fd
    #             print('mooo')
    #     return self

    async def __anext__(self):
        while True:
            if not self._lines and self._eof:
                raise StopAsyncIteration

            if self._index >= len(self._lines) - 1:
                tmp = await self.read(self._buffer_size)
                if tmp:
                    lines = tmp.splitlines()
                    self._index = 0
                    if self._lines:
                        result = self._lines[0] + lines[0]
                    else:
                        result = lines[0]
                    self._lines = lines[1:]
                    return result
                else:
                    result = self._lines[-1]
                    self._lines = []
                    return result
            else:
                result = self._lines[self._index]
                self._index += 1
                return result

            #
            # tokens = self._buffer.split(b'\n', 1)
            # if len(tokens) == 2:
            #     self._buffer = tokens[1]
            #     return tokens[0]
            # else:
            #     tmp = await self.read(self._buffer_size)
            #     if tmp:
            #         self._buffer += tmp
            #     else:
            #         if self._buffer:
            #             return self._buffer
            #         else:
            #             raise StopAsyncIteration

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