import aiofiles
import asyncstream
import asyncio


async def run():
    async with asyncstream.open('samples/animals.txt', mode='rb') as fd:
        async with asyncstream.open('samples/animals.txt.gz', mode='wb', compression='gzip') as gzfd:
            async for line in fd:
                await gzfd.write(line)


if __name__ == '__main__':
    asyncio.run(run())
