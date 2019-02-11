import os
import sys
import matplotlib.pyplot as plt 
import matplotlib.mlab as mlab 
import math 
import numpy as np 

def plot_inverse():
    x = x = np.linspace(0.4, 2.5, 100)
    y = 1/x

    fig, ax = plt.subplots()
    ax.plot(x, y, c='black')
    ax.set(xlabel='speed', ylabel='time')
    ax.tick_params(axis=u'both', which=u'both', length=0)
    plt.xlim([-0.1, 2.6])
    plt.ylim([-0.1, 2.6])
    # plt.xscale('log')
    # plt.yscale('log')

    #plt.show()
    plt.savefig('figs/inverse2.png', transparent=True)

def plot_norm():
    x = np.linspace(5 - 8, 5 + 8, 100)
    y1 = mlab.normpdf(x, 5, 1)
    y2 = mlab.normpdf(x, 5, 3)

    fig, ax = plt.subplots()
    ax.plot(x, y1, c='red')
    ax.plot(x, y2, c='blue')
    ax.tick_params(axis=u'both', which=u'both', direction='out', length=5)

    #plt.show()
    plt.savefig('figs/gaussian.png', transparent=True)

def main():
    plot_inverse()
    #plot_norm()

if __name__ == '__main__':
    main()