import csv
import io
import os
import tempfile

import aiofiles
import pytest

import asyncstream
from tests.data import test_utils
from tests.data.test_utils import async_gen_to_list


def get_raw_rows(filename: str):
    with open(filename) as fd:
        csv_reader = csv.reader(fd)
        return list(csv_reader)


def get_raw_lines(filename: str):
    with open(filename, 'rb') as fd:
        return list(fd)


@pytest.mark.parametrize(
    "compression,extension", [
        (None, ''),
        ('gzip', '.gz'),
        ('bzip2', '.bz2'),
        ('zstd', '.zst')
    ]
)
@pytest.mark.asyncio
async def test_open(compression: str, extension: str):
    baby_name_filename = os.path.join(os.path.dirname(__file__), 'data', 'baby_names.csv')
    decomp_func = getattr(test_utils, 'uncompress_' + str(compression).lower())
    async with aiofiles.open(baby_name_filename, 'rb') as fd:
        with tempfile.NamedTemporaryFile('wb') as tmpfd:
            async with aiofiles.open(tmpfd.name, 'wb') as atmpfd:
                async with asyncstream.open(atmpfd, 'wb', compression=compression) as gzfd:
                    async for line in fd:
                        await gzfd.write(line)
            result = decomp_func(tmpfd.name)
            assert get_raw_lines(baby_name_filename) == result
        # assert get_raw_rows(baby_name_filename) == await async_gen_to_list(asyncstream.reader(fd, compression=compression))

@pytest.mark.asyncio
async def test_write_parquet():
    baby_name_filename = os.path.join(os.path.dirname(__file__), 'data', 'baby_names.csv')
    async with aiofiles.open(baby_name_filename, 'rb') as fd:
        async with asyncstream.reader(fd, ignore_header=False) as reader:
            async with aiofiles.open('/tmp/abc.parquet', 'wb') as wfd:
                writer = asyncstream.writer(wfd, encoding='parquet', columns=await reader.header())
                async for row in reader:
                    writer.writerow(row)
                await writer.close()

# @pytest.mark.parametrize(
#     "compression,extension", [
#         (None, ''),
#         ('gzip', '.gz'),
#         ('bzip2', '.bz2'),
#         ('zstd', '.zst')
#     ]
# )
# @pytest.mark.asyncio
# async def test_open(compression: str, extension: str):
#     baby_name_filename = os.path.join(os.path.dirname(__file__), 'data', 'baby_names.csv')
#     async with aiofiles.open(baby_name_filename + extension, 'rb') as fd:
#         assert get_raw_lines(baby_name_filename) == await async_gen_to_list(asyncstream.open(fd, compression=compression))
