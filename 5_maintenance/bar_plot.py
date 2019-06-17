import os 
import pandas as pd 
import matplotlib
import matplotlib.pyplot as plt
from matplotlib.patches import Patch 
import numpy as np
import sys

matplotlib.rcParams['hatch.linewidth'] = 0.15
plt.rcParams.update({'font.size': 10, 'font.weight': "normal", 'font.family':'serif', 'axes.linewidth': 0.1})
pd.set_option('display.max_columns', 10)

absolute_path = os.path.dirname(os.path.abspath(__file__))

def get_tg0_data():
    scen12_data = pd.read_csv(absolute_path+'/output_march19/results/scen12_results.csv')
    scen34_data = pd.read_csv(absolute_path+'/output_march19/results/scen34_results.csv')
    scen12_data = scen12_data[scen34_data.columns]
    tg0_data = pd.concat([scen12_data, scen34_data])
    tg0_data['case'] = np.where(tg0_data['case']=='normal', 'nr',
        np.where(tg0_data['case']=='eco', 'em', tg0_data['case']))

    return tg0_data

def autolabel(ax, rects, xpos=0):
    """
    Attach a text label above each bar in *rects*, displaying its height.

    https://matplotlib.org/3.1.0/gallery/lines_bars_and_markers/barchart.html#sphx-glr-gallery-lines-bars-and-markers-barchart-py
    """

    for rect in rects:
        width = int(rect.get_width())
        if variable == 'pci': xpos=width*1.02
        ax.annotate('{:,}'.format(width),
                    xy=(xpos, rect.get_y()+rect.get_height()))

def main(variable, xlabel, color_local, color_highway):

    ### concatenate results from no traffic growth and traffic growth
    tg0_data = get_tg0_data()
    tg0_data['tg'] = 0
    tg1_data = pd.read_csv(absolute_path+'/output_Jun2019/results/scen_tg1_results.csv')
    tg1_data = tg1_data[tg0_data.columns]
    res_data = pd.concat([tg0_data, tg1_data])

    ### Only consider scenarios with IRI = 0.03
    sub_data = res_data[res_data['iri_impact']==0.03].copy()
    if variable != 'pci':
        sub_data_avg = sub_data.groupby(['tg', 'case', 'eco_route_ratio', 'budget']).agg({'{}_local'.format(variable): np.mean, '{}_highway'.format(variable): np.mean, '{}_total'.format(variable): np.mean}).reset_index(drop=False)
        sub_data_avg['{}_local'.format(variable)] = sub_data_avg['{}_local'.format(variable)].round(0)
        sub_data_avg['{}_highway'.format(variable)] = sub_data_avg['{}_highway'.format(variable)].round(0)
        sub_data_avg['{}_total'.format(variable)] = sub_data_avg['{}_total'.format(variable)].round(0)
    else:
        sub_data_avg = sub_data[sub_data['year']==10].copy()
        sub_data_avg['pci_local'] = sub_data_avg['pci_local'].round(0)
    xpos_local = np.min(sub_data_avg['{}_local'.format(variable)])/2
    xpos_highway = xpos_local*2 + np.min(sub_data_avg['{}_highway'.format(variable)])/2

    ### Set column for ordering rows by cases
    sort_map = {'nr': 0, 'em': 1, 'er': 2, 'ee': 3}
    sub_data_avg['case_order'] = sub_data_avg['case'].map(sort_map)
    
    def ratio(series):
        return (max(series)/min(series)-1)*100
    sub_data_avg_grp = sub_data_avg.sort_values(by='tg', ascending=True).groupby(['case', 'eco_route_ratio', 'budget']).agg({'{}_local'.format(variable): ratio, '{}_highway'.format(variable): ratio, '{}_total'.format(variable): ratio})
    print(sub_data_avg_grp)
    sys.exit(0)

    fig, ax = plt.subplots()
    fig.set_size_inches(7, 9)
    ind = [0, 1, 2, 3, 4, 5, 6, 7]
    shift = {(0,700): -0.3, (1, 700): -0.1, (0,200): 0.1, (1,200):0.3}
    color_local = color_local
    color_highway = color_highway
    hatch = {(0,700): None, (1, 700): None, (0,200): '///', (1,200): '///'}
    legend_elements_dict = {}

    plt_grp = sub_data_avg.groupby(['tg', 'budget'])
    for nm, grp in plt_grp:
        grp_x = [i+shift[nm] for i in ind]
        grp_data = grp.sort_values(['case_order', 'eco_route_ratio'], ascending=True)
        if nm[0]==0: tg_str='No traffic growth'
        else: tg_str='Traffc growth'

        rect_local = ax.barh(grp_x, grp_data['{}_local'.format(variable)], 0.2, label=nm, color = color_local[nm], hatch=hatch[nm])
        autolabel(ax, rect_local, xpos=xpos_local)
        legend_elements_dict['{}_{}_local'.format(nm[0], nm[1])] = Patch(facecolor = color_local[nm], edgecolor=None, hatch=hatch[nm], label='{}, budget {}, local roads'.format(tg_str, nm[1]))
        if variable != 'pci':
            rect_highway = ax.barh(grp_x, grp_data['{}_highway'.format(variable)], 0.2, left=grp_data['{}_local'.format(variable)], label=nm, color = color_highway[nm], hatch=hatch[nm])
            autolabel(ax, rect_highway, xpos=xpos_highway)
            legend_elements_dict['{}_{}_highway'.format(nm[0], nm[1])] = Patch(facecolor = color_highway[nm], edgecolor=None, hatch=hatch[nm], label='{}, budget {}, highways'.format(tg_str, nm[1]))

    ### Shrink current axis's height
    box = ax.get_position()
    #ax.set_position([box.x0, box.y0+box.height*0.2, box.width, box.height*0.9])
    ax.set_position([box.x0+box.width*0.1, box.y0+box.height*0.2, box.width*0.9, box.height*0.9])
    if variable != 'pci':
        legend_elements_list = [legend_elements_dict[('0_700_local')], legend_elements_dict['1_700_local'], legend_elements_dict[('0_200_local')], legend_elements_dict['1_200_local'], legend_elements_dict['0_700_highway'], legend_elements_dict['1_700_highway'],  legend_elements_dict['0_200_highway'], legend_elements_dict['1_200_highway']]
        legend = plt.legend(title = '', handles=legend_elements_list, bbox_to_anchor=(box.x0+box.width*1.3, box.y0-0.3*box.height), frameon=False, ncol=2, labelspacing=0.5)
    else:
        legend_elements_list = [legend_elements_dict[('0_700_local')], legend_elements_dict['1_700_local'], legend_elements_dict[('0_200_local')], legend_elements_dict['1_200_local']]
        legend = plt.legend(title = '', handles=legend_elements_list, bbox_to_anchor=(box.x0+box.width*0.9, box.y0-0.3*box.height), frameon=False, ncol=1, labelspacing=0.5)
        plt.xlim([0, 100])
    plt.gca().invert_yaxis()
    plt.xlabel(xlabel)
    plt.yticks(ind, ('Normal', 'Eco maint.', '10% eco routing', '50% eco routing', '100% eco routing', 'Eco maint. +\n10% eco routing', 'Eco maint. +\n50% eco routing', 'Eco maint. +\n100% eco routing'))
    #plt.show()
    plt.savefig(absolute_path + '/output_Jun2019/Figs/{}_tg.png'.format(variable), dpi=300, transparent=True)


if __name__ == '__main__':
    variable = 'vkmt'
    xlabel_dict = {
        'emi': 'Annual Average Daily CO\u2082 (t)',
        'vkmt': 'Annual Average Daily Vehicle \n Kilometers Travelled (AAD-VKMT)', 
        'vht': 'Annual Average Daily Vehicle \n Hours Travelled (AAD-VHT)', 
        'pci': 'Average Pavement Condition Index (PCI) of local roads'}
    color_local_dict = {
        'vkmt': {(0,700): (0.91,0.71,0.52,0.2), (1, 700): (0.53,0.35,0.47,0.2), (0,200): (0.91,0.71,0.52,0.2), (1,200): (0.53,0.35,0.47,0.2)},
        'vht': {(0,700): 'whitesmoke', (1, 700): 'mistyrose', (0,200): 'whitesmoke', (1,200): 'mistyrose'},
        'emi': {(0,700): 'lemonchiffon', (1, 700): 'lightskyblue', (0,200): 'lemonchiffon', (1,200): 'lightskyblue'},
        'pci': {(0,700): 'honeydew', (1, 700): 'darkseagreen', (0,200): 'honeydew', (1,200): 'darkseagreen'}
    }
    color_highway_dict = {
        'vkmt': {(0,700): (0.694,0.50,0.47,0.5), (1, 700): (0.294,0.161,0.353,0.5), (0,200): (0.694,0.50,0.47,0.5), (1,200): (0.294,0.161,0.353,0.5)},
        'vht': {(0,700): 'lightgray', (1, 700): 'lightsalmon', (0,200): 'lightgray', (1,200): 'lightsalmon'},
        'emi': {(0,700): 'gold', (1, 700): 'cornflowerblue', (0,200): 'gold', (1,200): 'cornflowerblue'},
        'pci': None
    } 
    main(variable, xlabel_dict[variable], color_local_dict[variable], color_highway_dict[variable])