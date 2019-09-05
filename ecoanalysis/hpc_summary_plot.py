import os
import sys
import numpy as np 
import pandas as pd 
import matplotlib.pyplot as plt 
import matplotlib.cm as cm

absolute_path = os.path.dirname(os.path.abspath(__file__))

plt.rcParams.update({'font.size': 15, 'font.weight': "normal", 'font.family':'serif', 'axes.linewidth': 0.1})

def traf_plot():
	pass

def emi_plot():

	fig, ax = plt.subplots(3, 3, figsize=(25, 25))
	color = iter(cm.viridis(np.linspace(0, 1, 3)))

	for case in ['nr', 'em', 'er']:

		c = next(color)
		emi_df = pd.read_csv(absolute_path + '/output_LCA2020/summary/hpc/emi_summary_c{}.csv'.format(case))
		ax[0, 0].plot(emi_df['year'], emi_df['emi_total'], c=c, label=case)
		ax[0, 1].plot(emi_df['year'], emi_df['emi_local'], c=c)
		ax[0, 2].plot(emi_df['year'], emi_df['emi_highway'], c=c)

		ax[1, 0].plot(emi_df['year'], emi_df['vht_total'], c=c)
		ax[1, 1].plot(emi_df['year'], emi_df['vht_local'], c=c)
		ax[1, 2].plot(emi_df['year'], emi_df['vht_highway'], c=c)

		ax[2, 0].plot(emi_df['year'], emi_df['pci_average'], c=c)
		ax[2, 1].plot(emi_df['year'], emi_df['pci_local'], c=c)
		ax[2, 2].plot(emi_df['year'], emi_df['pci_highway'], c=c)

	ax[0, 0].set(title='emi_total', xticks=np.arange(0, 2, 1), ylim=[100, 120])
	ax[0, 1].set(title='emi_local', xticks=np.arange(0, 2, 1))
	ax[0, 2].set(title='emi_highway', xticks=np.arange(0, 2, 1))

	ax[1, 0].set(title='vht_total', xticks=np.arange(0, 2, 1))
	ax[1, 1].set(title='vht_local', xticks=np.arange(0, 2, 1))
	ax[1, 2].set(title='vht_highway', xticks=np.arange(0, 2, 1))

	ax[2, 0].set(title='pci_average', xticks=np.arange(0, 2, 1))
	ax[2, 1].set(title='pci_local', xticks=np.arange(0, 2, 1))
	ax[2, 2].set(title='pci_highway', xticks=np.arange(0, 2, 1))

	handles, labels = ax[0, 0].get_legend_handles_labels()
	fig.legend(handles, labels, loc=(0.45, 0.05), ncol=3)
	plt.show()


if __name__ == '__main__':
	emi_plot()