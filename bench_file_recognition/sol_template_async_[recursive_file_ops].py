"""Written by Benjamin Jack Cullen
Intention: My first unique multi-processed async program.
Setup: I made this setup for advanced, extremely fast mass file operation(s).
"""
import os
import time
import magic
import codecs
import asyncio
import aiofiles
from aiomultiprocess import Pool
import multiprocessing


def scantree(path):
    """Recursively yield DirEntry objects for given directory."""
    try:
        for entry in os.scandir(path):
            if entry.is_dir(follow_symlinks=False):
                yield from scantree(entry.path)
            else:
                yield entry
    except:
        pass


def scan(pn, _d, path):
    fp = []
    for entry in scantree(path):
        if entry.is_file():
            fp.append([entry.path])
    _d[pn] = fp


def chunk_data(data, chunk_size) -> list:
    _chunks = [data[x:x + chunk_size] for x in range(0, len(data), chunk_size)]
    data = []
    for _chunk in _chunks:
        data.append(_chunk)
    return data


def unchunk_data(data, depth=1):
    new_data = data
    for i in range(0, depth):
        new_sub_data = []
        for dat in new_data:
            for x in dat:
                new_sub_data.append(x)
        new_data = new_sub_data
    return new_data


def read_buff(file):
    b = ''
    try:
        b = magic.from_buffer(codecs.open(file, "rb").read(int(1024)))
    except:
        b = magic.from_buffer(open(file, "r").read(int(1024)))
    return b


async def stat_file(file) -> list:
    try:
        return [file, os.path.getsize(file), await asyncio.to_thread(read_buff, file)]
    except:
        pass


async def call_stat(x) -> list:
    fsz = []
    for _ in x:
        fsz.append(await stat_file(_))
    return fsz


async def main(_chunks) -> list:
    async with Pool() as pool:
        results = await pool.map(call_stat, _chunks)
    return results


if __name__ == '__main__':
    _manager = multiprocessing.Manager()
    _d = _manager.dict()

    # Pre-scan
    target = 'C:\\'
    t = time.perf_counter()
    paths = next(os.walk(target))[1]
    results = []
    p = []
    [p.append(multiprocessing.Process(target=scan, args=(pn, _d, target + str(paths[pn])))) for pn in range(len(paths))]
    [x.start() for x in p]
    [x.join() for x in p]
    results.append(_d.values())
    results = unchunk_data(results, depth=3)
    [results.append(target + f) for f in os.listdir(target) if os.path.isfile(target + f)]
    print('[pre-scan] time:', time.perf_counter() - t)
    print('[files]', len(results))
    # print(results)

    # # Setup
    proc_max = 8
    n_chunks = int(abs(len(results) / proc_max))+1
    chunks = chunk_data(results, n_chunks)

    # Entry point
    t = time.perf_counter()
    res = asyncio.run(main(chunks))
    # print(res)
    print('[multi-process+async] time:', time.perf_counter()-t)
