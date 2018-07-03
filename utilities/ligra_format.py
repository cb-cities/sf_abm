### Graph properties
import sys
import igraph
import numpy as np 

def main():
    ### Read initial graph
    in_filename='London_Directed/London_0621.graphmlz'#'Imputed_data_False9_0509.graphmlz'
    weight_attr='length'#'sec_length'
    out_filename='London_Directed/London_0621.tsv'

    g = igraph.load('data_repo/'+in_filename) ### This file contains the weekday 9am link level travel time for SF, imputed data collected from a month worth of Google Directions API
    print(g.summary())

    s_list = [e.source for e in g.es]
    t_list = [e.target for e in g.es]
    w_list = g.es[weight_attr]

    print(len(s_list), len(t_list), len(w_list))

    ligra_array = np.column_stack((s_list, t_list, w_list))
    ligra_array = ligra_array[np.lexsort((ligra_array[:,1], ligra_array[:,0]))]
    print(ligra_array.shape)
    print(ligra_array[0:10,:])

    np.savetxt('data_repo/'+out_filename, ligra_array, fmt='%d\t%d\t%.6f')
    

if __name__ == '__main__':
    main()

