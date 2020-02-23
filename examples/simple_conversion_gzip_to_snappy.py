import asyncstream
import asyncio

async def run():
    async with asyncstream.open('samples/animals.txt.gz', 'rb', compression='gzip') as inc_fd:
        async with asyncstream.open('samples/animals.txt.snappy', 'wb', compression='snappy') as outc_fd:
            async for line in inc_fd:
                await outc_fd.write(line)


if __name__ == '__main__':
    asyncio.run(run())
