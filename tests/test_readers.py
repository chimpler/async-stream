import csv
import os
import aiofiles
import pyarrow.parquet
import pytest

import asyncstream
from tests.data.test_utils import async_gen_to_list


def get_raw_rows(filename: str):
    with open(filename, 'r') as fd:
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
async def test_reader(compression: str, extension: str):
    baby_name_filename = os.path.join(os.path.dirname(__file__), 'data', 'baby_names.csv')
    async with aiofiles.open(baby_name_filename + extension, 'rb') as fd:
        assert get_raw_rows(baby_name_filename) == await async_gen_to_list(asyncstream.reader(fd, compression=compression))


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
    async with aiofiles.open(baby_name_filename + extension, 'rb') as fd:
        assert get_raw_lines(baby_name_filename) == await async_gen_to_list(asyncstream.open(fd, compression=compression))

# def test_read_parquet2():
#     with open('/Users/fdang/tmp/parquet2/0016_part_00.parquet', 'rb') as fd:
#         import pandas as pd
#         # table = pd.read_parquet('/Users/fdang/tmp/parquet2/0016_part_00.parquet', engine='pyarrow')
#         table = pd.read_parquet(fd, engine='pyarrow')
#         # table = pyarrow.parquet.read_table(fd)
#         print(table)
#         assert False

@pytest.mark.asyncio
async def test_read_parquet():
    baby_name_filename = os.path.join(os.path.dirname(__file__), 'data', 'baby_names.csv')
    # '~/tmp/parquet2/0016_part_00.parquet'
    # async with aiofiles.open('/Users/fdang/tmp/parquet2/0016_part_00.parquet', 'rb') as fd:
    async with aiofiles.open('/tmp/abc.parquet', 'rb') as fd:
        async with asyncstream.reader(fd, encoding='parquet', ignore_header=False) as reader:
            assert get_raw_rows(baby_name_filename) == [(await reader.header())] + await async_gen_to_list(reader)
