async-stream
============

    DO NOT USE - in development


Simple library to compress/uncompress Async streams using file iterator and readers.

## Examples

### Simple gzip compression

Compress a regular file to gzip:
```python
import aiofiles
import asyncstream
import asyncio

async def run():
    async with aiofiles.open('examples/animals.txt', 'rb') as fd:
        async with aiofiles.open('/tmp/animals.txt.gz', 'wb') as wfd:
            async with asyncstream.open(wfd, 'wb', compression='gzip') as gzfd:
                async for line in fd:
                    await gzfd.write(line)

asyncio.run(run())
```

It also supports other compression scheme such as: `bzip2`, `snappy`, `zstd`. More will be added soon!

### Convert a gzip file to a bzip2 file

```python
import aiofiles
import asyncstream
import asyncio

async def run():
    async with aiofiles.open('/tmp/animals.txt.gz', 'rb') as in_fd:
        async with aiofiles.open('/tmp/animals.txt.bz2', 'wb') as out_fd:
            async with asyncstream.open(in_fd, 'rb', compression='gzip') as inc_fd:
                async with asyncstream.open(out_fd, 'wb', compression='bzip2') as outc_fd:
                    async for line in inc_fd:
                        await outc_fd.write(line)

asyncio.run(run())
```

### Use an async reader and writer to filter and update data on the fly
 ```python
import aiofiles
import asyncstream
import asyncio
 
async def run():
    async with aiofiles.open('/tmp/animals.txt.bz2', 'rb') as in_fd:
        async with aiofiles.open('/tmp/animals.txt.snappy', 'wb') as out_fd:
            async with asyncstream.reader(in_fd, compression='bzip2') as reader:
                async with asyncstream.writer(out_fd, compression='snappy') as writer:
                    async for name, color, age in reader:
                        if color != 'white':
                            await writer.writerow([name, color, age * 2])
 
asyncio.run(run())
```

### Simple parquet encoding

Compress a regular file to `parquet` using the compression `snappy`:

```python
import aiofiles
import asyncstream
import asyncio

async def run():
    async with aiofiles.open('examples/animals.txt', 'rb') as fd:
        async with aiofiles.open('output.parquet', 'wb') as wfd:
            async with asyncstream.writer(wfd, encoding='parquet', compression='snappy') as writer:
                async for line in fd:
                    await writer.write(line)

asyncio.run(run())
```

### Simple parquet decoding

### Simple orc encoding

### Simple orc decoding

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
`gzip` / `zlib`                          | :white_check_mark:
`bzip2`                          | :white_check_mark:
`snappy`                          | :white_check_mark:
`zstd`                          | :white_check_mark:

### Parquet
Compression                                  | Status
-------------------------------------- | :-----:
none                    | :white_check_mark:
`brotli`                  | :white_check_mark:
`bzip2`                  | :x: 
`gzip`                  | :x: 
`snappy`                  | :white_check_mark:
`zstd`                  | :x:
`zlib`                  | :white_check_mark:

### Orc

Compression                       | Status
-------------------------------------- | :-----:
none                    | :white_check_mark:
`bzip2`                  | :x: 
`gzip` / `zlib`                 | :white_check_mark:
`snappy`                  | :white_check_mark:
`zlib`                  | :white_check_mark:
`zstd`                  | :white_check_mark:
