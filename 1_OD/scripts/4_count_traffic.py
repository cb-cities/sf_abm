import pandas as pd 
import numpy as np 
import os
import sys
import matplotlib.pyplot as plt 

plt.rcParams.update({'font.size': 10, 'font.weight': "normal", 'font.family':'serif', 'axes.linewidth': 0.1})

absolute_path = os.path.dirname(os.path.abspath(__file__))

def main(year, day):
    OD_count = 0
    intraSF_OD_count = 0
    intercity_OD_count = 0

    for hour in range(3, 27):
        intraSF_OD_df = pd.read_csv(open(absolute_path+'/../output/OD_tables_growth/intraSF/SF_OD_YR{}_DY{}_HR{}.csv'.format(year, day, hour)))
        try:
            OD_count += np.sum(intraSF_OD_df['flow'])
            intraSF_OD_count += np.sum(intraSF_OD_df['flow'])
        except KeyError:
            OD_count += intraSF_OD_df.shape[0]
            intraSF_OD_count += intraSF_OD_df.shape[0]

        intercity_OD_df = pd.read_csv(open(absolute_path+'/../output/OD_tables_growth/intercity/intercity_YR{}_HR{}.csv'.format(year, hour)))
        try:
            OD_count += np.sum(intercity_OD_df['flow'])
            intercity_OD_count += np.sum(intercity_OD_df['flow'])
        except KeyError:
            OD_count += intercity_OD_df.shape[0]
            intercity_OD_count += intercity_OD_df.shape[0]

    return OD_count, intraSF_OD_count, intercity_OD_count

def plot_growth():
    OD_count_df = pd.read_csv(absolute_path+'/../output/OD_count.csv')
    fig, ax = plt.subplots()
    fig.set_size_inches(6, 4.5)
    ax.plot(OD_count_df['year'], OD_count_df['intraSF'], c='red', marker='.', label='Intra-city trips')
    ax.plot(OD_count_df['year'], OD_count_df['intercity'], c='blue', marker='.', label='Intercity trips')
    ax.plot(OD_count_df['year'], OD_count_df['total'], c='black', marker='.', label='Total trips')
    plt.xlabel('Year')
    plt.ylabel('Trip counts')
    plt.ylim([100000, 2000000])
    plt.ticklabel_format(style='sci', axis='y', scilimits=(0,0))
    ### Shrink current axis's height
    box = ax.get_position()
    #ax.set_position([box.x0, box.y0+box.height*0.2, box.width, box.height*0.9])
    ax.set_position([box.x0, box.y0+box.height*0.15, box.width, box.height*0.9])
    plt.legend(title = '', bbox_to_anchor=(box.x0+box.width*1.1, box.y0-0.3*box.height), frameon=False, ncol=3, labelspacing=0.5)
    # plt.show()
    plt.savefig(absolute_path + '/../output/count_traffic.png', dpi=300, transparent=True)

if __name__ == '__main__':
    
    # OD_count_list = []
    # for year in range(11):
    #     OD_count, intraSF_OD_count, intercity_OD_count = main(year, 2)
    #     OD_count_list.append((year, OD_count, intraSF_OD_count, intercity_OD_count))
    # OD_count_df = pd.DataFrame(OD_count_list, columns=['year', 'total', 'intraSF', 'intercity'])
    # OD_count_df.to_csv('OD_count.csv', index=False)

    plot_growth()
