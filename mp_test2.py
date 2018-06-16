#!/usr/bin/python

import time
import itertools
import threading
import multiprocessing
import random


def f(x):
    return x

def ngrams(input_tmp, n):
    input = input_tmp.split()

    if n > len(input):
        n = len(input)

    output = []
    for i in range(len(input)-n+1):
        output.append(input[i:i+n])
    return output 

def foo():

    p = multiprocessing.Pool()
    mapper = p.imap_unordered

    num = 100000000 #100
    rand_list = random.sample(xrange(100000000), num)

    rand_str = ' '.join(str(i) for i in rand_list)

    for n in xrange(1, 100):
        res = list(mapper(f, ngrams(rand_str, n)))


if __name__ == '__main__':
    start = time.time()
    foo()
    print 'Total time taken: '+str(time.time() - start)