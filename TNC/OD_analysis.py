import pandas as pd 
import json 
import sys 
import matplotlib.path as mpltPath
import numpy as np 
import scipy.sparse 

def find_in_nodes(row, points, nodes_df):
    if row['geometry'].type == 'MultiPolygon':
        return []
    else:
        path = mpltPath.Path(list(zip(*row['geometry'].exterior.coords.xy)))
        in_index = path.contains_points(points)
        return nodes_df['index'].loc[in_index].tolist()

def TAZ_nodes():
    ### Find corresponding nodes for each TAZ
    ### Save as 'taz_nodes.geojson'
    taz_gdf = gpd.read_file('TAZ981/TAZ981.shp')
    taz_gdf = taz_gdf.to_crs({'init': 'epsg:4326'})
    # print(taz_gdf.dtypes)
    # print(taz_gdf.iloc[0,1])
    # print(taz_gdf.crs)
    # print(taz_gdf.shape)
    # for i in range(len(taz_gdf['geometry'])):
    #     if taz_gdf['geometry'].iloc[i].type == 'MultiPolygon':
    #         print(taz_gdf['TAZ'].iloc[i])

    nodes_dict = json.load(open('tagged_alloneway_nodes.json'))
    nodes_df = pd.DataFrame.from_dict(nodes_dict, orient='index', columns=['lat', 'lon']).reset_index()
    # print(nodes_df.head())

    points = nodes_df[['lon', 'lat']].values
    taz_gdf['in_nodes'] = taz_gdf.apply(lambda row: find_in_nodes(row, points, nodes_df), axis=1)
    with open('taz_nodes.geojson', 'w') as outfile:
        outfile.write(taz_gdf.to_json())

def OD_population(row, OD_string):
    ### Assign TAZ level pickups and dropoffs randomly to nodes
    ### row[OD_string] is the scale factor which the sum of normalized array equals to.
    unnormalized = np.random.random(row['no_of_nodes'])
    normalized = row[OD_string]*unnormalized/unnormalized.sum()
    return normalized.tolist()

def OD_iterations(OD, target_O, target_D):
    #print('OD_matrix', OD_matrix.shape, type(OD_matrix))

    D_sum = np.asarray(OD.sum(axis=0)).flatten() ### Current dropoffs at each node, needs to be updated by target_D
    D_coef = target_D/(D_sum+0.01)
    OD.data *= np.take(D_coef, OD.indices)

    OD = OD.tocsc()
    O_sum = np.asarray(OD.sum(axis=1)).flatten()
    O_coef = target_O/(O_sum+0.01)
    OD.data *= np.take(O_coef, OD.indices)
    OD = OD.tocsr()

    D_sum = np.asarray(OD.sum(axis=0)).flatten()
    O_sum = np.asarray(OD.sum(axis=1)).flatten()
    errors = np.sum(np.abs(target_D-D_sum)) + np.sum(np.abs(target_O-O_sum))

    return OD, errors


def TAZ_nodes_OD(day, hour):
    ### [{'taz': 1, 'in_nodes': [...]}, ...]
    taz_nodes_data = json.load(open('taz_nodes.geojson'))
    taz_nodes_data = taz_nodes_data['features']
    taz_nodes_list = [{'taz': record['properties']['TAZ'], 'in_nodes': record['properties']['in_nodes']} for record in taz_nodes_data]
    taz_nodes_df = pd.DataFrame(taz_nodes_list)

    ### [{'taz':1, 'day':1, 'hour':10, 'pickups': 80, 'dropoffs': 100}, ...]
    ### Monday is 0 -- Sunday is 6. Hour is from 3am-26am(2am next day)
    OD_df = pd.read_csv('taz_OD.csv')
    hour_OD_df = OD_df[(OD_df['day_of_week']==day) & (OD_df['hour']==hour)]

    nodes_OD_df = taz_nodes_df.merge(hour_OD_df, on='taz')
    nodes_OD_df['no_of_nodes'] = nodes_OD_df.apply(lambda row: int(len(row['in_nodes'])), axis=1)
    ### filter out rows that have no nodes in them (nodes are not computed for these rows because the geometries are multipolygons rather than polygons)
    nodes_OD_df = nodes_OD_df[nodes_OD_df['no_of_nodes'] != 0]

    ### Assign the TAZ level pickups and dropoffs to nodes
    nodes_OD_df['O_nodes'] = nodes_OD_df.apply(lambda row: OD_population(row, 'pickups'), axis=1)
    nodes_OD_df['D_nodes'] = nodes_OD_df.apply(lambda row: OD_population(row, 'dropoffs'), axis=1)

    ### Construct an array and a dictionary of all the nodes
    node_list = np.concatenate(nodes_OD_df['in_nodes'].tolist())
    node_dict = {i:node_list[i] for i in range(len(node_list))}

    ### Construct an array of targeted pickups and dropoffs at each node
    target_O = np.concatenate(nodes_OD_df['O_nodes'].tolist())
    target_D = np.concatenate(nodes_OD_df['D_nodes'].tolist())
    # print('Dimensions of target_O and target_D', target_O.shape, target_D.shape)
    # print('Total trips: ', np.sum(target_O))

    ###############################
    ########## Sparse OD ##########
    ###############################
    ### Initialize the OD matrix, that will be iterated to match the target_O (row sums) and target_D (column sums)
    OD_size = len(node_list)
    OD_density = 10*10e-6
    OD_matrix = scipy.sparse.random(OD_size, OD_size, density=OD_density, format='csr')
    print(len(OD_matrix.data), len([i for i in OD_matrix.data if i>0]))
    # print('Agent counts', len(OD_matrix.data), max(OD_matrix.data), min(OD_matrix.data))
    ### Add a small diagonal elements to avoid division-by-zero errors
    OD_matrix += scipy.sparse.diags(
        [0.001*np.ones(OD_size-1), 0.001*np.ones(OD_size-1)],
        [1,-1])
    print(len(OD_matrix.data), len([i for i in OD_matrix.data if i>0]))
    #sys.exit(0)
    # print('Agent counts', len(OD_matrix.data), max(OD_matrix.data), min(OD_matrix.data))
    errors_list = []
    for i in range(20):
        OD_matrix, errors = OD_iterations(OD_matrix, target_O, target_D)
        errors_list.append(errors)
        #print('errors at iteration {}: {}'.format(i, errors))
    print(OD_matrix.shape, len(OD_matrix.data), len([i for i in OD_matrix.data if i>0]))
    OD_matrix.eliminate_zeros()
    print(OD_matrix.shape, len(OD_matrix.data), len([i for i in OD_matrix.data if i>0]))

    ### Save hourly sparse OD matrix
    scipy.sparse.save_npz('OD_matrices/DY{}_HR{}_OD.npz'.format(day, hour), OD_matrix)

    with open('OD_matrices/DY{}_HR{}_node_dict.json'.format(day, hour), 'w') as outfile:
        json.dump(node_dict, outfile, indent=2)

    meta_file = {'day_M0_S6': day, 'hour': hour, 'total_trips': np.sum(target_O), 'agent_counts': len(OD_matrix.data), 'errors_list': errors_list, 'unmatched_trips': errors}
    print(meta_file)
    with open('OD_matrices/DY{}_HR{}_meta.json'.format(day, hour), 'w') as outfile:
        json.dump(meta_file, outfile, indent=2)


if __name__ == '__main__':
    #TAZ_nodes()
    for day_of_week in [1,6]: ### Two typical days, 1 for Monday (weekday) and 6 for Sdunday (weekend)
        for hour in [3]:#range(4,27): ### 24 hour-slices per day
            ### Monday is 0 -- Sunday is 6. Hour is from 3am-26am(2am next day)
            TAZ_nodes_OD(day_of_week, hour)