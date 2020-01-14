async-stream
============

    DO NOT USE - in development


Simple library to compress/uncompress Async streams.

## Examples

### Simple compression

Compress a regular file to gzip:
```python
import aiofiles
import asyncstream
import asyncio

async def run():
    async with aiofiles.open('input.txt', 'rb') as fd:
        async with aiofiles.open('output.txt.gz', 'wb') as wfd:
            async with asyncstream.open(wfd, 'wb', compression='gzip') as gzfd:
                async for line in fd:
                    await gzfd.write(line)

asyncio.run(run())
```

It also supports other compression scheme such as: `bzip2`, `snappy`, `zstd`. More will be added soon!

### Simple parquet encoding

Compress a regular file to `parquet` using the compression `snappy`:

```python
import aiofiles
import asyncstream
import asyncio

async def run():
    async with aiofiles.open('input.txt', 'rb') as fd:
        async with aiofiles.open('output.parquet', 'wb') as wfd:
            async with asyncstream.open(wfd, 'wb', encoding='parquet', compression='snappy') as pqfd:
                async for line in fd:
                    await pqfd.write(line)

asyncio.run(run())
```

Other compression scheme are supported: `zlib`, `brotli`.

### Simple gunzip from s3

Using `aiobotocore`, one can simply uncompress files from s3:
import aiobotocore
import asyncstream
import asyncio

async def run():
    session = aiobotocore.get_session()
    async with session.create_client('s3') as s3:
    obj = await s3.get_object(Bucket='s3_bucket', Key='s3_key')
    async with asyncstream.open(obj['Body'], 'rb', compression='gzip') as fd:
        async for line in fd:
            print(line)
    
asyncio.run(run()) 

## Compression supported

Compression                                  | Status
-------------------------------------- | :-----:
`gzip`                          | :white_check_mark:
`bzip2`                          | :white_check_mark:
`snappy`                          | :white_check_mark:
`zstd`                          | :white_check_mark:

### Encoding supported
Encoding                                  | Status
-------------------------------------- | :-----:
`parquet`                          | :white_check_mark:
o `snappy`                  | :white_check_mark:
o `zlib`                  | :white_check_mark:
o `brotli`                  | :white_check_mark:
`orc`                          | :white_check_mark:
o `snappy`                  | :white_check_mark:
o `zlib`                  | :white_check_mark:
o `zstd`                  | :white_check_mark:
o `gzip`                  | :white_check_mark:
