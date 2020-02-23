import aiofiles
import asyncstream
import asyncio


async def run():
    async with aiofiles.open('samples/animals.txt', 'rb') as fd:
        async with aiofiles.open('samples/animals2.txt.gz', 'wb') as wfd:
            async with asyncstream.open(wfd, 'wb', compression='gzip') as gzfd:
                async for line in fd:
                    await gzfd.write(line)


if __name__ == '__main__':
    asyncio.run(run())
