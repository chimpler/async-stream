from typing import Optional, Iterable, List, Any

from asyncstream import AsyncFileObj


class AsyncWriter(object):
    def __init__(self, afd: AsyncFileObj, columns=Optional[Iterable[str]], column_types=Optional[Iterable[str]], has_header=False, sep=b',', eol=b'\n'):
        self._afd = afd
        self._sep = sep
        self._eol = eol
        self._columns = columns
        self._column_types = column_types
        self._has_header = has_header

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return await self._afd.close()

    async def writeheader(self):
        await self._afd.write(self._sep.join(self._columns) + self._eol)

    async def writerow(self, row: List[Any]):
        await self._afd.write(self._sep.join(row) + self._eol)

    async def writerows(self, rows: List[List[Any]]):
        for row in rows:
            await self.writerow(row)

    async def flush(self):
        await self._afd.flush()
