### Graph properties
import sys
import igraph
import matplotlib.pyplot as plt 

def graph_process():

    import json
    import pprint
    import glob
    import gzip

    node_data = json.load(gzip.open('data_repo/London_Directed/roadnodes1.json.gz'))
    print(len(node_data))
    pprint.pprint(node_data[0])

    link_data = []
    for file in glob.glob('data_repo/London_Directed/' + "roadlinks*.json.gz"):
        links_t_f = gzip.open(file)
        links_t = json.load(links_t_f)
        link_data += [{'OS_toid': edge['OS_toid'], 'positiveNode': edge['positiveNode'], 'negativeNode': edge['negativeNode'], 'length': edge['length']} for edge in links_t]       
        print(len(links_t), len(link_data))

    g = igraph.Graph.DictList(
        vertices=node_data,
        edges=link_data, 
        vertex_name_attr="toid",
        edge_foreign_keys=('negativeNode',"positiveNode"),
        directed=True)

    g.write_graphmlz('data_repo/London_Directed/London_0621.graphmlz')

def main():
    ### Read initial graph
    #g = igraph.load('data_repo/Imputed_data_False9_0509.graphmlz') ### This file contains the weekday 9am link level travel time for SF, imputed data collected from a month worth of Google Directions API
    g = igraph.load('data_repo/London_Directed/London_0621.graphmlz')
    print(g.summary())

    mode = 'ALL' ### 'IN', 'OUT', 'ALL'
    degree_dist = g.degree_distribution(mode=mode)
    loc = []
    val = []
    for bn in degree_dist.bins():
        loc.append(bn[0])
        val.append(bn[2])
    print(loc, val)

    plt.bar(loc, val, width=1, color='w', edgecolor='black', log=True)
    for i, v in enumerate(val):
        plt.text(i-0.25, v,  str(v), color='blue', fontweight='bold')
    plt.xlim([-1,(int(max(loc))+1)])
    plt.title('Degree distribution of mode {}'.format(mode))
    plt.xlabel('Degree')
    plt.ylabel('Vertices count')
    plt.show()
    

if __name__ == '__main__':
    graph_process()
    #main()

