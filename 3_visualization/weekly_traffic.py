import json
import sys
import numpy as np 
import pandas as pd 
import os 
import matplotlib.pyplot as plt 
import matplotlib.cm as cm 

absolute_path = os.path.dirname(os.path.abspath(__file__))

folder = 'sf_overpass/original'

plt.rcParams.update({'font.size': 15, 'font.weight': "normal", 'font.family':'serif', 'axes.linewidth': 0.1})

def weekly_traffic(case):

    ### Aggregate hourly flow to weekly flow
    network_attr_df = pd.read_csv(absolute_path+'/../0_network/data/{}/edges_elevation.csv'.format(folder))
    network_attr_df = network_attr_df[['edge_id_igraph', 'length', 'geometry']]
    network_attr_df[case] = 0

    random_seed = 0
    probe_ratio = 1
    if case=='sevenday_traffic':
        day_list = [0, 1, 2, 3, 4, 5, 6]
    else:
        day_list = [0, 1, 2, 3, 4]

    for day in day_list:
        for hour in range(3, 27):
            hour_edge_flow_df = pd.read_csv(absolute_path+'/../2_ABM/output/edges_df/edges_df_DY{}_HR{}_r{}_p{}.csv'.format(day, hour, random_seed, probe_ratio))
            network_attr_df = pd.merge(network_attr_df, hour_edge_flow_df[['edge_id_igraph', 'true_flow']], on = ['edge_id_igraph'])
            network_attr_df[case] += network_attr_df['true_flow']
            network_attr_df = network_attr_df[['edge_id_igraph', 'length', case]]

    ### Directed flow
    edge_flow_df.to_csv('directed_{}.csv'.format(case), index=False)

    ### Merge results from two directions
    edge_flow_df['edge_id_igraph_str'] = edge_flow_df['edge_id_igraph'].astype(str)
    edge_flow_df['undir_uv_igraph'] = pd.DataFrame(np.sort(edge_flow_df[['start_igraph', 'end_igraph']].values, axis=1), columns=['small_igraph', 'large_igraph']).apply(lambda x:'%s_%s' % (x['small_igraph'],x['large_igraph']),axis=1)
    edge_flow_df_grp = edge_flow_df.groupby('undir_uv_igraph').agg({
            case: np.sum, 
            'edge_id_igraph_str': lambda x: '-'.join(x),
            'geometry': 'first'}).reset_index()
    edge_flow_df_grp = edge_flow_df_grp.rename(columns={case: 'undirected_{}'.format(case)})

    edge_flow_df_grp.to_csv('undirected_{}.csv'.format(case), index=False)

def delay_time():

    time_df = pd.DataFrame(columns=['edge_id_igraph', 'day', 'hour', 't_avg'])

    selected_edges = ['25578', '6020', '7929']
    street_labels = {'25578': 'James Lick Fwy (US 101)', '6020': 'Park Presidio Blvd (CA 1)', '15838': 'Balboa St', '13965': 'Monterey Blvd', '7929': '14th Ave', '15907': 'I 280', '4950': 'Hayes St'}

    random_seed = 0
    probe_ratio = 1
    for day in [0, 1, 2, 3, 4]:
        for hour in range(3, 27):
            hour_edge_flow_df = pd.read_csv(absolute_path+'/../2_ABM/output/edges_df/edges_df_DY{}_HR{}_r{}_p{}.csv'.format(day, hour, random_seed, probe_ratio))
            selected_edges_df = hour_edge_flow_df.loc[hour_edge_flow_df['edge_id_igraph'].isin(selected_edges)].copy()
            selected_edges_df['day'] = day
            selected_edges_df['hour'] = hour
            time_df = pd.concat([time_df, selected_edges_df[['edge_id_igraph', 'day', 'hour', 't_avg']]])

    network_attr_df = pd.read_csv(absolute_path+'/../0_network/data/{}/edges_elevation.csv'.format(folder))
    network_attr_df = network_attr_df[['edge_id_igraph', 'length', 'fft']]
    time_df['edge_id_igraph'] = time_df['edge_id_igraph'].astype(int)
    time_df = pd.merge(time_df, network_attr_df, on='edge_id_igraph', how='left')
    time_df['delay'] = time_df['t_avg']/time_df['fft']
    time_df['day_hour'] = 24*time_df['day'] + time_df['hour']

    fig, ax = plt.subplots()
    fig.set_size_inches(10, 7)
    color = iter(cm.rainbow(np.linspace(0, 1, 3)))

    for name, group in time_df.groupby('edge_id_igraph'):
        c = next(color)
        ax.plot('day_hour', 'delay', data=group, color=c, label=street_labels[str(name)])
    
    plt.xticks(range(3, 112, 12), ['Mon\n 6 AM', 'Mon\n 6 PM', 'Tue\n 6 AM', 'Tue\n 6 PM', 'Wed\n 6 AM', 'Wed\n 6 PM', 'Thu\n 6 AM', 'Thu\n 6 PM', 'Fri\n 6 AM', 'Fri\n 6 PM'])
    ### Shrink current axis's height
    box = ax.get_position()
    ax.set_position([box.x0, box.y0+box.height*0.15, box.width, box.height*0.9])
    legend = plt.legend(bbox_to_anchor=(0.5, -0.3), loc='lower center', frameon=False, ncol=3, labelspacing=1.5)
    plt.ylabel('Delay: time in traffic / free flow time')
    plt.show()


if __name__ == '__main__':
    weekly_traffic('sevenday_traffic')
    #delay_time()



