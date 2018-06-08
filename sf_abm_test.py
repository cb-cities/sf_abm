import multiprocessing
from multiprocessing import Pool
import time
import multiporcessing

# def f(x):
#     logging.info(multiprocessing.current_process().name)
#     logging.info(x)
#     time.sleep(10)
#     logging.info(multiprocessing.current_process().name)

def chunks(vcount, n):
    for i in range(0, vcount, n):
        yield range(i, i+n)

# logging.basicConfig(filename='sample.log', level=logging.INFO)
# logging.info(multiprocessing.cpu_count())
# pool = Pool(processes=4)
# logging.info("Pool initialized")
# pool.map(f, [100,200,300,400], chunksize=1)
# pool.close()
# pool.join()

vcount = 200 #OD_matrix.shape[0]
partitioned_v = list(chunks(vcount, int(vcount/4)))
print('vertices partition finished')
print(partitioned_v)
