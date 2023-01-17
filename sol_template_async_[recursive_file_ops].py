"""Written by Benjamin Jack Cullen
Intention: My first unique multi-processed async program.
Setup: I made this setup for advanced, extremely fast mass file operation(s).
"""
import asyncio
import os
import time
import multiprocessing
from aiomultiprocess import Pool


def chunk_data(data, chunk_size) -> list:
    _chunks = [data[x:x + chunk_size] for x in range(0, len(data), chunk_size)]
    data = []
    for _chunk in _chunks:
        data.append(_chunk)
    return data


async def stat_file(file) -> list:
    return [file, os.path.getsize(file)]


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

    # Build File List
    fp = []
    for diName, subdirname, filelist in os.walk('D:\\'):
        for fname in filelist:
            fp.append(os.path.join(diName, fname))

    # Setup
    proc_max = 8
    n_chunks = int(abs(len(fp) / proc_max))+1
    chunks = chunk_data(fp, n_chunks)
    # print('Files:', len(fp))
    # print('Files in each chunk:', n_chunks)
    # print('Number of chunks:', len(chunks))

    # Entry point
    t = time.perf_counter()
    res = asyncio.run(main(chunks))
    print('time:', time.perf_counter()-t)
