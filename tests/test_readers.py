import csv
import os
import aiofiles
import pytest

from asyncstream import reader
from tests.data.test_utils import async_gen_to_list

def get_raw_rows(filename: str):
    with open(filename) as fd:
        csv_reader = csv.reader(fd)
        return list(csv_reader)

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
        assert get_raw_rows(baby_name_filename) == await async_gen_to_list(reader(fd, compression=compression))
