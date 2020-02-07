# import csv
# import os
# import tempfile
#
# import aiofiles
# import pytest
#
# import asyncstream
# from tests import test_utils
# from tests.test_utils import decode_parquet, decode_orc
#
import os
from tempfile import NamedTemporaryFile

import aiofiles
import pytest

import asyncstream
from tests.test_utils import get_raw_lines


@pytest.mark.parametrize(
    "compression", [
        None,
        'gzip',
        'bzip2',
        'zstd'
    ]
)
@pytest.mark.asyncio
async def test_writer(compression: str):
    baby_name_filename = os.path.join(os.path.dirname(__file__), 'data', 'baby_names.csv')
    with NamedTemporaryFile(mode='wb', delete=False) as tmpfd:
        async with aiofiles.open(baby_name_filename, 'rb') as rfd:
            async with aiofiles.open(tmpfd.name, 'wb') as owfd:
                async with asyncstream.open(owfd, mode='wb', compression=compression) as wfd:
                    async for line in rfd:
                        await wfd.write(line)
        assert get_raw_lines(baby_name_filename) == get_raw_lines(tmpfd.name, compression)

#
# def get_raw_rows(filename: str):
#     with open(filename) as fd:
#         csv_reader = csv.reader(fd)
#         return list(csv_reader)
#
#
# def get_raw_lines(filename: str):
#     with open(filename, 'rb') as fd:
#         return list(fd)
#
#
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
#     decomp_func = getattr(test_utils, 'uncompress_' + str(compression).lower())
#     async with aiofiles.open(baby_name_filename, 'rb') as fd:
#         with tempfile.NamedTemporaryFile('wb') as tmpfd:
#             async with aiofiles.open(tmpfd.name, 'wb') as atmpfd:
#                 async with asyncstream.open(atmpfd, 'wb', compression=compression) as gzfd:
#                     async for line in fd:
#                         await gzfd.write(line)
#             result = decomp_func(tmpfd.name)
#             assert get_raw_lines(baby_name_filename) == result
#
# @pytest.mark.parametrize(
#     "compression", [
#         None,
#         'snappy',
#         'gzip',
#         'brotli'
#     ]
# )
# @pytest.mark.asyncio
# async def test_write_parquet(compression: str):
#     baby_name_filename = os.path.join(os.path.dirname(__file__), 'data', 'baby_names.csv')
#     async with aiofiles.open(baby_name_filename, 'rb') as fd:
#         async with asyncstream.reader(fd, has_header=True) as reader:
#             with tempfile.NamedTemporaryFile() as tmpfd:
#                 async with aiofiles.open(tmpfd.name, 'wb') as wfd:
#                     writer = asyncstream.writer(wfd, encoding='parquet', columns=await reader.header())
#                     async for row in reader:
#                         writer.writerow(row)
#                     await writer.close()
#                 assert get_raw_lines(baby_name_filename) == decode_parquet(tmpfd.name).encode('utf-8').splitlines(True)
#
# # @pytest.mark.parametrize(
# #     "compression", [
# #         None,
# #         'snappy',
# #         'gzip'
# #     ]
# # )
# # @pytest.mark.asyncio
# # async def test_write_orc(compression: str):
# #     baby_name_filename = os.path.join(os.path.dirname(__file__), 'data', 'baby_names.csv')
# #     async with aiofiles.open(baby_name_filename, 'rb') as fd:
# #         async with asyncstream.reader(fd, ignore_header=False) as reader:
# #             with tempfile.NamedTemporaryFile() as tmpfd:
# #                 async with aiofiles.open(tmpfd.name, 'wb') as wfd:
# #                     writer = asyncstream.writer(wfd, encoding='orc', columns=await reader.header())
# #                     async for row in reader:
# #                         writer.writerow(row)
# #                     await writer.close()
# #                 assert get_raw_lines(baby_name_filename) == decode_orc(tmpfd.name).encode('utf-8').splitlines(True)
