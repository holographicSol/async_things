"""Written by Benjamin Jack Cullen
Intention: Bench.
Setup: Multiprocess + Async for faster mass file operation(s).
"""
import os
import time
import magic
import codecs
import asyncio
from aiomultiprocess import Pool
import multiprocessing


def scantree(path: str) -> str:
    try:
        for entry in os.scandir(path):
            if entry.is_dir(follow_symlinks=False):
                yield from scantree(entry.path)
            else:
                yield entry
    except:
        pass


def scan(path: str) -> list:
    fp = []
    for entry in scantree(path):
        if entry.is_file():
            fp.append([entry.path])
    return fp


def chunk_data(data: list, chunk_size: int) -> list:
    _chunks = [data[x:x + chunk_size] for x in range(0, len(data), chunk_size)]
    data = []
    for _chunk in _chunks:
        data.append(_chunk)
    return data


def un_chunk_data(data: list, depth: int) -> list:
    new_data = data
    for i in range(0, depth):
        new_sub_data = []
        for dat in new_data:
            for x in dat:
                new_sub_data.append(x)
        new_data = new_sub_data
    return new_data


def file_sub_ops(file: str) -> str:
    try:
        buff = magic.from_buffer(codecs.open(file, "rb").read(int(1024)))
    except:
        buff = magic.from_buffer(open(file, "r").read(int(1024)))
    return buff


async def file_ops(file: str) -> list:
    try:
        return [file, os.path.getsize(file), await asyncio.to_thread(file_sub_ops, file)]
    except:
        pass


async def entry_point(chunk: list) -> list:
    return [await file_ops(item) for item in chunk]


async def main(_chunks: list) -> list:
    async with Pool() as pool:
        _results = await pool.map(entry_point, _chunks)
    return _results


if __name__ == '__main__':
    _manager = multiprocessing.Manager()
    _d = _manager.dict()

    # target directory
    target = 'D:\\'

    # pre-scan: (linear synchronous single process pre-scan to compile a file list ready for multiproc+async ops.)
    t = time.perf_counter()
    files = scan(path=target)
    files = un_chunk_data(files, depth=1)
    print('[pre-scan] time:', time.perf_counter() - t)

    # # Setup
    print('[files]', len(files))
    proc_max = 16
    chunks = chunk_data(files, proc_max)
    print('[number of expected chunks]', len(chunks))

    # main operation: multiprocess+async
    t = time.perf_counter()
    results = asyncio.run(main(chunks))
    # print(results)
    print('[multi-process+async] time:', time.perf_counter()-t)
