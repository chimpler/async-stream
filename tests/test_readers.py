import csv
import io
import os
from tempfile import SpooledTemporaryFile, NamedTemporaryFile

import aiofiles
import pyarrow.parquet
import pytest

import asyncstream
from tests.data import test_utils
from tests.data.test_utils import async_gen_to_list, compress_gzip, encode_parquet


def get_raw_rows(filename: str):
    with open(filename, 'r') as fd:
        csv_reader = csv.reader(fd)
        return list(csv_reader)


def get_raw_lines(filename: str):
    with open(filename, 'rb') as fd:
        return list(fd)


@pytest.mark.parametrize(
    "compression", [
        None,
        'gzip',
        'bzip2',
        'zstd'
    ]
)
@pytest.mark.asyncio
async def test_reader(compression: str):
    baby_name_filename = os.path.join(os.path.dirname(__file__), 'data', 'baby_names.csv')
    with NamedTemporaryFile() as tmpfd:
        compressed_fd = getattr(test_utils, 'compress_' + str(compression).lower())(baby_name_filename)
        tmpfd.write(compressed_fd)
        tmpfd.flush()
        async with aiofiles.open(tmpfd.name, 'rb') as fd:
            assert get_raw_rows(baby_name_filename) == await async_gen_to_list(asyncstream.reader(fd, compression=compression))


@pytest.mark.parametrize(
    "compression", [
        None,
        'gzip',
        'bzip2',
        'zstd'
    ]
)
@pytest.mark.asyncio
async def test_open(compression: str):
    baby_name_filename = os.path.join(os.path.dirname(__file__), 'data', 'baby_names.csv')
    with NamedTemporaryFile() as tmpfd:
        compressed_fd = getattr(test_utils, 'compress_' + str(compression).lower())(baby_name_filename)
        tmpfd.write(compressed_fd)
        tmpfd.flush()
        async with aiofiles.open(tmpfd.name, 'rb') as fd:
            assert get_raw_lines(baby_name_filename) == await async_gen_to_list(asyncstream.open(fd, compression=compression))

@pytest.mark.parametrize(
    "compression", [
        None,
        'snappy',
        'gzip',
        'brotli'
    ]
)
@pytest.mark.asyncio
async def test_read_parquet(compression: str):
    baby_name_filename = os.path.join(os.path.dirname(__file__), 'data', 'baby_names.csv')
    with NamedTemporaryFile() as tmpfd:
        tmpfd.write(encode_parquet(baby_name_filename, compression=compression))
        tmpfd.flush()

        async with aiofiles.open(tmpfd.name, 'rb') as fd:
            async with asyncstream.reader(fd, encoding='parquet', compression=compression, ignore_header=False) as reader:
                assert get_raw_rows(baby_name_filename) == [(await reader.header())] + await async_gen_to_list(reader)
