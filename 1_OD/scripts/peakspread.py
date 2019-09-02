import os 
import gc 
import sys 
import numpy as np 
import pandas as pd 
from matplotlib import cm 
import matplotlib.pyplot as plt 

absolute_path = os.path.dirname(os.path.abspath(__file__))
plt.rcParams.update({'font.size': 15, 'font.weight': "normal", 'font.family':'serif', 'axes.linewidth': 0.1})

def plot_demand_profile(spread=True):

	od_counts = []
	for year in range(11):
		for day in [2]:
			for hour in range(3, 27):

				if spread:
					intra_od = pd.read_csv(absolute_path+'/../output/sf_overpass/peakspread/SF_OD_YR{}_DY{}_HR{}.csv'.format(year, day, hour))
					inter_od = pd.read_csv(absolute_path+'/../output/sf_overpass/peakspread/intercity_YR{}_HR{}.csv'.format(year, hour))
					try:
						spread_od = pd.read_csv(absolute_path+'/../output/sf_overpass/peakspread/spread_YR{}_HR{}.csv'.format(year, hour))
						od_counts.append([year, day, hour, intra_od.shape[0], inter_od.shape[0], spread_od.shape[0]])
					except FileNotFoundError:
						od_counts.append([year, day, hour, intra_od.shape[0], inter_od.shape[0], 0])
				else:
					intra_od = pd.read_csv(absolute_path+'/../output/sf_overpass/intraSF_growth/SF_OD_YR{}_DY{}_HR{}.csv'.format(year, day, hour))
					inter_od = pd.read_csv(absolute_path+'/../output/sf_overpass/intercity_growth/intercity_YR{}_HR{}.csv'.format(year, hour))
					od_counts.append([year, day, hour, intra_od.shape[0], inter_od.shape[0], 0])
				gc.collect()

	od_counts_df = pd.DataFrame(od_counts, columns=['year', 'day', 'hour', 'intra', 'inter', 'spread'])
	od_counts_df['total'] = od_counts_df['intra'] + od_counts_df['inter'] + od_counts_df['spread']

	fig, ax = plt.subplots()
	color = iter(cm.viridis(np.linspace(0, 1, 11)))
	for year in range(11):

		one_year = od_counts_df[od_counts_df['year']==year]
		c = next(color)
		ax.plot('hour', 'total', data=one_year, c=c)

	plt.axvline(x=7)
	plt.axvline(x=9)
	plt.axvline(x=17)
	plt.axvline(x=19)
	plt.xticks(np.arange(3, 27, 1))
	plt.show()

def creat_peak_spread_od():
	
	for year in range(11):
		for day in [2]:

			spread_list = []
			for hour in range(3, 27):

				intra_od = pd.read_csv(absolute_path+'/../output/sf_overpass/intraSF_growth/SF_OD_YR{}_DY{}_HR{}.csv'.format(year, day, hour))
				inter_od = pd.read_csv(absolute_path+'/../output/sf_overpass/intercity_growth/intercity_YR{}_HR{}.csv'.format(year, hour))
				gc.collect()

				if hour in [7, 8, 9, 17, 18, 19]:
					intra_od['keep'] = np.random.choice([0, 1], size=intra_od.shape[0], p=[0.1, 0.9])
					inter_od['keep'] = np.random.choice([0, 1], size=inter_od.shape[0], p=[0.1, 0.9])

					spread_list.append(intra_od[intra_od['keep']==0])
					spread_list.append(inter_od[inter_od['keep']==0])

					intra_od.loc[intra_od['keep']==1][['O', 'D']].to_csv(absolute_path+'/../output/sf_overpass/peakspread/SF_OD_YR{}_DY{}_HR{}.csv'.format(year, day, hour), index=False)
					inter_od.loc[inter_od['keep']==1][['O', 'D']].to_csv(absolute_path+'/../output/sf_overpass/peakspread/intercity_YR{}_HR{}.csv'.format(year, hour), index=False)
				else:
					intra_od.to_csv(absolute_path+'/../output/sf_overpass/peakspread/SF_OD_YR{}_DY{}_HR{}.csv'.format(year, day, hour), index=False)
					inter_od.to_csv(absolute_path+'/../output/sf_overpass/peakspread/intercity_YR{}_HR{}.csv'.format(year, hour), index=False)

			spread_od = pd.concat(spread_list, sort=False)
			spread_hours = [10, 11, 12, 13, 14, 15, 16]
			spread_od['new_hour'] = np.random.choice(spread_hours, size=spread_od.shape[0], p=[1/7]*7)
			for s_h in spread_hours:
				spread_od.loc[spread_od['new_hour']==s_h][['O', 'D']].to_csv(absolute_path+'/../output/sf_overpass/peakspread/spread_YR{}_HR{}.csv'.format(year, s_h), index=False)

if __name__ == '__main__':

	plot_demand_profile(spread=False)
	#creat_peak_spread_od()