"""Written by Benjamin Jack Cullen
Intention: Experimentation. Benchmark mass file operation(s).
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


def multi_task(_pn, _d, _path) -> list:
    """ tag the results with p_num """
    _results = []
    for dirName, subdirList, fileList in os.walk(_path):
        for fname in fileList:
            fpath = os.path.join(dirName, fname)
            sz = os.path.getsize(fpath)
            _results.append([fpath, sz])
    _d[_pn] = [_pn, _results]


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


async def make_file_list():
    fp = []
    for diName, subdirname, filelist in os.walk('D:\\'):
        for fname in filelist:
            fp.append(os.path.join(diName, fname))
    return fp


async def mainasync():
    x = await make_file_list()
    mt = []
    for _ in x:
        mt.append(await stat_file(_))


if __name__ == '__main__':

    dsk = 'C:\\'
    paths = next(os.walk(dsk))[1]
    print(paths)

    """ single process """
    t = time.perf_counter()
    _results = []
    for dirName, subdirList, fileList in os.walk(dsk):
        for fname in fileList:
            fpath = os.path.join(dirName, fname)
            sz = os.path.getsize(fpath)
            _results.append([fpath, sz])
    print('time (single process):', time.perf_counter()-t)

    """ multiprocess """
    _manager = multiprocessing.Manager()
    _d = _manager.dict()
    t = time.perf_counter()
    results = []
    p = []
    [p.append(multiprocessing.Process(target=multi_task, args=(pn, _d, dsk+str(paths[pn])))) for pn in range(len(paths))]
    [x.start() for x in p]
    [x.join() for x in p]
    results.append(_d.values())
    print('time (multi-process):', time.perf_counter() - t)

    """ async """
    t = time.perf_counter()
    asyncio.run(mainasync())
    print('time (async):', time.perf_counter() - t)

    """ multiprocess + async """
    fp = []
    for diName, subdirname, filelist in os.walk('D:\\'):
        for fname in filelist:
            fp.append(os.path.join(diName, fname))
    proc_max = 8
    n_chunks = int(abs(len(fp) / proc_max))+1
    chunks = chunk_data(fp, n_chunks)
    t = time.perf_counter()
    res = asyncio.run(main(chunks))
    print('time (multi-process async):', time.perf_counter()-t)

# time (single process): 47.9288760000054
# time (multi-process): 25.16215380000358
