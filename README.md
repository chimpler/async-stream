async-stream
============

Simple library to compress/uncompress Async streams using file iterator and readers.

It supports the following compression format:

* gzip
* bzip2
* snappy
* zstandard
* parquet (experimental)
* orc (experimental)

### Getting started

Install the library as follows:

    pip install asyncstream

Compress a regular file to gzip:
```python
import aiofiles
import asyncstream
import asyncio

async def run():
    async with asyncstream.open('examples/animals.txt', 'rb') as fd:
        async with asyncstream.open('examples/animals.txt.gz', 'wb', compression='gzip') as gzfd:
            async for line in fd:
                await gzfd.write(line)

asyncio.run(run())
```

or you can also open from an async file descriptor using aiofiles:

    pip install aiofiles
    
And then run the following code:        
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

You can also uncompress an S3 file on the fly using aiobotocore:

    pip install aiobotocore

And then run the following code:
```python
import aiobotocore
import asyncstream
import asyncio

async def run():
    session = aiobotocore.get_session()
    async with session.create_client('s3') as s3:
    obj = await s3.get_object(Bucket='test-bucket', Key='path/to/file.gz')
    async with asyncstream.open(obj['Body'], 'rt', compression='gzip') as fd:
        async for line in fd:
            print(line)
    
asyncio.run(run()) 
```

### Convert a gzip file to a bzip2 file

```python
import asyncstream
import asyncio

async def run():
    async with asyncstream.open('/tmp/animals.txt.gz', 'rb', compression='gzip') as inc_fd:
        async with asyncstream.open('/tmp/animals.txt.bz2', 'wb', compression='bzip2') as outc_fd:
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
    async with asyncstream.open('/tmp/animals.txt.bz2', 'rb') as in_fd:
        async with asyncstream.open('/tmp/animals.txt.snappy', 'wb') as out_fd:
            async with asyncstream.reader(in_fd) as reader:
                async with asyncstream.writer(out_fd) as writer:
                    async for name, color, age in reader:
                        if color != 'white':
                            await writer.writerow([name, color, age * 2])
 
asyncio.run(run())
```

### Simple parquet encoding

Compress a regular file to `parquet` using the compression `snappy`:

```python
import asyncstream
import asyncio

async def run():
    async with asyncstream.open('examples/animals.txt', 'rb') as fd:
        async with asyncstream.open('output.parquet', 'wb', encoding='parquet', compression='snappy') as wfd:
            async with asyncstream.writer(wfd) as writer:
                async for line in fd:
                    await writer.write(line)

asyncio.run(run())
```

### Simple parquet decoding
```python
import asyncstream
import asyncio

async def run():
    async with asyncstream.open('output.parquet', 'rb', encoding='parquet') as fd:
            async with asyncstream.reader(fd) as reader:
                async for line in reader:
                    print(line)

asyncio.run(run())
```

### Simple orc encoding
```python
import asyncstream
import asyncio

async def run():
    async with asyncstream.open('examples/animals.txt', 'rb') as fd:
        async with asyncstream.open('output.orc.snappy', 'wb', encoding='orc', compression='snappy') as wfd:
            async with asyncstream.writer(wfd) as writer:
                async for line in fd:
                    await writer.write(line)

asyncio.run(run())
```
### Simple orc decoding

```python
import asyncstream
import asyncio

async def run():
    async with asyncstream.open('output.orc.snappy', 'rb', encoding='orc') as fd:
            async with asyncstream.reader(fd) as reader:
                async for line in reader:
                    print(line)

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
