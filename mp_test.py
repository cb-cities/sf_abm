from multiprocessing import Pool
import time
import os

def f1(x):
    print('map', x*x, os.getpid())

def f2(x):
    #print('map_async', x*x, os.getpid())
    return reduce(lambda a, b: math.log(a+b), xrange(10**5), x)

def main():
    pool = Pool(processes=4)
    #pool.map(f1, range(10))
    r  = pool.map_async(f2, range(50000))
    # DO STUFF
    print('HERE')
    print('MORE')
    r.wait()
    print('DONE')

if __name__ == '__main__':
    main()