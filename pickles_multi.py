
from multiprocessing import Pool

import pathlib
import shutil


import contextlib

LOGS_PATH = pathlib.Path('/datax/scratch/andresl/xband-pickles-logs')
if LOGS_PATH.is_dir():
    shutil.rmtree(LOGS_PATH)
LOGS_PATH.mkdir()

NUM_PROCESSES = 1
MAX_RSS = int(10 * 10**9) # so 80 GB

NUM_BATCHES = 100 # this seems like the kind of temporary hack that is never really temporary...

def pickles_worker(batch_num):
    import pickles
    import resource
    MAX_MEMORY_PER_WORKER = MAX_RSS // NUM_PROCESSES
    resource.setrlimit(resource.RLIMIT_AS, (MAX_MEMORY_PER_WORKER,) * 2)

    with open(LOGS_PATH / f'batch_{batch_num:>02}.log', 'w') as f:
        with contextlib.redirect_stdout(f):
            pickles.main(str(batch_num), 'True', 'False')

print('Running...')

with Pool(processes = NUM_PROCESSES) as pool:
    pool.map(pickles_worker, range(NUM_BATCHES))

print('Done!')