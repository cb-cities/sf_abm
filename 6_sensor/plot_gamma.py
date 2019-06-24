import os
import sys
import pandas as pd 
import numpy as np 
import matplotlib.pyplot as plt 
import matplotlib.cm as cm
from matplotlib.lines import Line2D
from pandas.plotting import parallel_coordinates
import scipy.stats as stats

def main():
    x = np.linspace(0,20,200)
    y1 = stats.gamma.pdf(x, a=0.5, scale=2)
    y2 = stats.gamma.pdf(x, a=1, scale=1)
    y3 = stats.gamma.pdf(x, a=2, scale=1)
    plt.plot(x, y1)
    plt.plot(x, y2)
    plt.plot(x, y3)
    plt.show()

if __name__ == '__main__':
    main()