import pickles

from multiprocessing import Pool

import pathlib
import shutil

import resource

import contextlib

LOGS_PATH = pathlib.Path('/datax/scratch/andresl/xband-pickles-logs')
if LOGS_PATH.is_dir():
    shutil.rmtree(LOGS_PATH)
LOGS_PATH.mkdir()

# resource management stuff. make lower to keep matt happier
NUM_PROCESSES = 16 
MAX_RSS = int(80 * 10**9) # so 80 GB
resource.setrlimit(resource.RLIMIT_AS, (MAX_RSS,) * 2)

NUM_BATCHES = 100 # this seems like the kind of temporary hack that is never really temporary...

def pickles_worker(batch_num):
    with open(LOGS_PATH / f'batch_{batch_num:>02}.log', 'w') as f:
        with contextlib.redirect_stdout(f):
            pickles.main(str(batch_num), 'True', 'False')

print('Running...')

with Pool(processes = NUM_PROCESSES) as pool:
    pool.map(pickles_worker, range(NUM_BATCHES))

print('Done!')