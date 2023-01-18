import os
import time
import multiprocessing
import string
import random
import magic
import codecs


def chunk_data(data, chunk_size):
    chunks = [data[x:x + chunk_size] for x in range(0, len(data), chunk_size)]
    data = []
    for chunk in chunks:
        data.append(chunk)
    return data


def unchunk_data(data, depth=1):
    """ Un-chunk the data when and if required.
    Data : list/list(s) of list(s). list[].
    Depth : Iterations / Cycles. int.
    """

    new_data = data
    for i in range(0, depth):
        new_sub_data = []
        for dat in new_data:
            # print(dat)
            try:
                for x in dat:
                    new_sub_data.append(x)
            except:
                pass
        new_data = new_sub_data
    return new_data


def task(_path) -> list:
    """ tag the results with p_num """
    _results = []
    for dirName, subdirList, fileList in os.walk(_path):
        for fname in fileList:
            fp = (os.path.join(dirName, fname))
            sz = os.path.getsize(fp)
            mt = os.path.getmtime(fp)
            b = ''
            try:
                b = magic.from_buffer(codecs.open(fp, "rb").read(int(1024)))
            except:
                pass
            _results.append([sz, mt, b, fp])

    return _results


def multi_task(_pn, _d, _path) -> list:
    """ tag the results with p_num """
    _results = []
    for dirName, subdirList, fileList in os.walk(_path):
        for fname in fileList:
            fp = (os.path.join(dirName, fname))
            sz = os.path.getsize(fp)
            b = ''
            try:
                b = magic.from_buffer(codecs.open(fp, "rb").read(int(1024)))
            except:
                b = magic.from_buffer(open(fp, "r").read(int(1024)))
            _results.append([fp, sz, b])
    _d[_pn] = [_pn, _results]


if __name__ == '__main__':

    paths = next(os.walk('E:\\'))[1]
    print(paths)

    """ single process """
    # t = time.perf_counter()
    # s_results = task(dsk)
    # print('items (single process):', len(s_results))
    # print('time (single process):', time.perf_counter()-t)
    # print()

    """ multiprocess """
    _manager = multiprocessing.Manager()
    _d = _manager.dict()
    t = time.perf_counter()
    p = []
    [p.append(multiprocessing.Process(target=multi_task, args=(pn, _d, 'E:\\'+str(paths[pn])))) for pn in range(len(paths))]
    [x.start() for x in p]
    [x.join() for x in p]
    # print(_d.values())
    print('time (multi-process):', time.perf_counter() - t)

# ToDo: time magic.from_buffer(file, 1024bytes)
# C:\
# (single process)
# (multi-process) 427.24449170001026
# (multi-proc async) 306.5057891000033 seconds

# Archives
# (multi-process): 1091.9271980000049 seconds (18 minutes)
# (multi-proc async): 181.32360679999692 seconds (3 minutes)

# steam games
# (multi-process):
# (multi-proc async): 1125.1655388000072 seconds

# music
# (multi-process): 13.090736499987543 seconds
# (multi-proc async): 1.7063694999960717 seconds

# windows
# (multi-process):
# (multi-proc async): 187.07587539999804 seconds

# documents
# (multi-process): 167.37757350000902 seconds
# (multi-proc async): 71.58939510000346 seconds

# D:\
# (multi-process): 97.92057139999815 seconds
# (multi-proc async): 69.14312890000292 seconds

# E:\
# (multi-process): 923.0067295999906 seconds
# (multi-proc async): 128.1552331999992 seconds

#
# (multi-process):  seconds
# (multi-proc async):  seconds
