import csv
import os
import aiofiles
import pytest

import asyncstream
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
