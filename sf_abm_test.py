import multiprocessing
from multiprocessing import Pool
import time
import logging

def f(x):
    logging.info(multiprocessing.current_process().name)
    logging.info(x)
    time.sleep(10)
    logging.info(multiprocessing.current_process().name)

logging.basicConfig(filename='sample.log', level=logging.INFO)
logging.info(multiprocessing.cpu_count())
pool = Pool(processes=4)
logging.info("Pool initialized")
pool.map(f, [100,200,300,400], chunksize=1)
pool.close()
pool.join()


