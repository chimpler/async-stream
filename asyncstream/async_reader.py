from typing import Optional, Iterable

from asyncstream import AsyncFileObj


class AsyncReader(object):
    def __init__(self, afd: AsyncFileObj, columns=Optional[Iterable[str]], column_types=Optional[Iterable[str]], has_header=False, sep=',', eol='\n'):
        self._afd = afd
        self._sep = sep
        self._eol = eol
        self._columns = columns
        self._column_types = column_types
        self._has_header = has_header

    async def __aenter__(self):
        return self

    def __aiter__(self):
        return self

    async def __aexit__(self, *exc):
        return None

    async def __anext__(self):
        next_line = await self._afd.__anext__()
        if next_line:
            return next_line.decode('utf-8').rstrip(self._eol).split(self._sep)
        else:
            raise StopAsyncIteration

