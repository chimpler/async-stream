import aiobotocore
import asyncstream
import asyncio

async def run():
    session = aiobotocore.get_session()
    async with session.create_client('s3') as s3:
        obj = await s3.get_object(Bucket='test-bucket', Key='path/to/file.gz')
        async with asyncstream.open(obj['Body'], 'rt', compression='bzip2') as fd:
            async for line in fd:
                print(line)
    

if __name__ == '__main__':
    asyncio.run(run())
