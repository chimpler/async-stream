import csv
import os
import aiofiles
import pytest

from asyncstream import reader
from tests.data.test_utils import async_gen_to_list

baby_name_filename = os.path.join(os.path.dirname(__file__), 'data', 'baby_names.csv')

@pytest.mark.asyncio
async def test_raw():
    with open(baby_name_filename) as fd:
        csv_reader = csv.reader(fd)
        expected_rows = list(csv_reader)

        async with aiofiles.open(baby_name_filename, 'rb') as fd:
            assert expected_rows == await async_gen_to_list(reader(fd))
