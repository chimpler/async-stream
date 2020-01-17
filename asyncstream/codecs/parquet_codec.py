import zlib

import zstd


def get_parquet_writer():

def get_parquet_reader():
    with SpooledTemporaryFile(mode='w+b', max_size=buffer_memory) as wfd:
        async for buf in fd:
            wfd.write(buf)

        wfd.flush()
        wfd.seek(0)
        return pd.read_parquet(wfd, engine='pyarrow')
