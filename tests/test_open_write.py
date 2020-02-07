import csv
import os
from tempfile import NamedTemporaryFile

import aiofiles
import pyorc
import pytest

import asyncstream
from tests import test_utils
from tests.test_utils import async_gen_to_list, encode_parquet, encode_orc, get_raw_rows, get_raw_lines, compress


@pytest.mark.parametrize(
    "compression", [
        None,
        'gzip',
        'bzip2',
        'zstd',
        'snappy'
    ]
)
@pytest.mark.asyncio
async def test_open_write(compression: str):
    baby_name_filename = os.path.join(os.path.dirname(__file__), 'data', 'baby_names.csv')
    with NamedTemporaryFile() as tmpfd:
        async with aiofiles.open(tmpfd.name, 'wb') as rfd:
            async with aiofiles.open(baby_name_filename, 'rb') as cfd:
                async with asyncstream.open(rfd, mode='wb', compression=compression) as fd:
                    await fd.write(await cfd.read())
                assert get_raw_lines(baby_name_filename) == get_raw_lines(tmpfd.name, compression)

@pytest.mark.parametrize(
    "compression", [
        None,
        'gzip',
        'bzip2',
        'zstd',
        'snappy'
    ]
)
@pytest.mark.asyncio
async def test_open_read_with_filename(compression: str):
    baby_name_filename = os.path.join(os.path.dirname(__file__), 'data', 'baby_names.csv')
    with NamedTemporaryFile() as tmpfd:
        async with aiofiles.open(baby_name_filename, 'rb') as cfd:
            async with asyncstream.open(tmpfd.name, mode='wb', compression=compression) as fd:
                await fd.write(await cfd.read())
            assert get_raw_lines(baby_name_filename) == get_raw_lines(tmpfd.name, compression)
