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


def scan(pn: int, _d: dict, path: str, multiproc=False) -> list:
    fp = []
    for entry in scantree(path):
        if entry.is_file():
            fp.append([entry.path])
    if multiproc is True:
        _d[pn] = fp
    else:
        return fp


def chunk_data(data: list, chunk_size: int) -> list:
    _chunks = [data[x:x + chunk_size] for x in range(0, len(data), chunk_size)]
    data = []
    for _chunk in _chunks:
        data.append(_chunk)
    return data


def unchunk_data(data: list, depth: int) -> list:
    new_data = data
    for i in range(0, depth):
        new_sub_data = []
        for dat in new_data:
            for x in dat:
                new_sub_data.append(x)
        new_data = new_sub_data
    return new_data


def read_buff(file: str) -> str:
    try:
        buff = magic.from_buffer(codecs.open(file, "rb").read(int(1024)))
    except:
        buff = magic.from_buffer(open(file, "r").read(int(1024)))
    return buff


async def stat_file(file: str) -> list:
    try:
        return [file, os.path.getsize(file), await asyncio.to_thread(read_buff, file)]
    except:
        pass


async def call_stat(chunk: list) -> list:
    _results = []
    for _ in chunk:
        _results.append(await stat_file(_))
    return _results


async def main(_chunks: list) -> list:
    async with Pool() as pool:
        _results = await pool.map(call_stat, _chunks)
    return _results


if __name__ == '__main__':
    _manager = multiprocessing.Manager()
    _d = _manager.dict()

    # target directory
    target = 'D:\\Music\\'

    # pre-scan: (linear synchronous) requires multiproc=False in scan(). caution.
    t = time.perf_counter()
    results = scan(pn=None, _d=None, path=target, multiproc=False)
    results = unchunk_data(results, depth=1)
    print('[pre-scan] time:', time.perf_counter() - t)

    # pre-scan: (multiprocess). requires multiproc=True in scan(). caution.
    # t = time.perf_counter()
    # paths = next(os.walk(target))[1]
    # results = []
    # p = []
    # [p.append(multiprocessing.Process(target=scan, args=(pn, _d, str(target+str(paths[pn]))))) for pn in range(len(paths))]
    # [x.start() for x in p]
    # [x.join() for x in p]
    # results.append(_d.values())
    # results = unchunk_data(results, depth=3)
    # [results.append(target + f) for f in os.listdir(target) if os.path.isfile(target + f)]
    # print('[pre-scan] time:', time.perf_counter() - t)
    # print('[files]', len(results))
    # print(results)

    # # Setup
    proc_max = 8
    n_chunks = int(abs(len(results) / proc_max))+1
    chunks = chunk_data(results, n_chunks)

    # main operation: multiprocess+async
    t = time.perf_counter()
    results = asyncio.run(main(chunks))
    # print(results)
    print('[multi-process+async] time:', time.perf_counter()-t)
