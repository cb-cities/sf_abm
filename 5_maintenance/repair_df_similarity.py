import pandas as pd 
import numpy as np 
import sys 
import os 

absolute_path = os.path.dirname(os.path.abspath(__file__))

def main(year):
	preprocessing_df = pd.read_csv(open(absolute_path+'/output_Jun2019/preprocessing.csv'))
	tg0_data = pd.read_csv(open(absolute_path+'/output_Jun2019/repair_df/repair_df_y{}_cee_tg0_b700_e1.0_i0.03.csv'.format(year)))
	tg1_data = pd.read_csv(open(absolute_path+'/output_Jun2019/repair_df/repair_df_y{}_cee_tg1_b700_e1.0_i0.03.csv'.format(year)))
	tg0_data = pd.merge(tg0_data, preprocessing_df[['edge_id_igraph', 'cnn_expand']], on='edge_id_igraph', how='left')
	tg1_data = pd.merge(tg1_data, preprocessing_df[['edge_id_igraph', 'cnn_expand']], on='edge_id_igraph', how='left')
	tg0_repair = np.unique(tg0_data['cnn_expand'].values)
	tg1_repair = np.unique(tg1_data['cnn_expand'].values)

	common_roads = np.intersect1d(tg0_repair, tg1_repair, assume_unique=True)
	print(year, len(common_roads), len(tg0_repair), len(tg1_repair), len(common_roads)/len(tg0_repair))

if __name__ == '__main__':
	for year in range(11):
		main(year)