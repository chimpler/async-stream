#!/usr/bin/env python
import sys

from setuptools import find_packages
from setuptools import setup
from setuptools.command.test import test as TestCommand


class PyTestCommand(TestCommand):
    user_options = [('pytest-args=', 'a', "Arguments to pass to py.test")]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.pytest_args = []

    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        import pytest
        errno = pytest.main(self.pytest_args)
        sys.exit(errno)


setup(
    name='async-stream',
    version='1.0.0',
    description='Async s3 stream',
    long_description='Async s3 stream',
    keywords='async stream compression gzip bzip2 zstd parquet orc',
    author='Francois Dang Ngoc',
    url='http://github.com/chimpler/async-stream/',
    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3'
    ],
    install_requires=[
        'zstandard',
        'uvloop',
        'aiofiles',
        'pyarrow==0.14.1',
        'pyorc',
        'pandas',
        'python-snappy'
    ],
    packages=find_packages(),
    tests_require=[
        'pytest-asyncio',
        'pytest-cov'
    ],
    entry_points={
        'console_scripts': [
        ]
    },
    cmdclass={
        'test': PyTestCommand
    }
)
