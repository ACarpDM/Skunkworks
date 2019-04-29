import numpy as np
import pandas as pd
import os

#### These things change depending on the dataset ####
def nothing(val):
	return val
def C2K(celsius):
	return celsius+273

base = os.path.expanduser("~/")
base = base + "Documents/Coleg/Skunkworks/MolProps/Datasets/"
# base = 'C:/Users/Jimbo/Documents/Coleg/Skunkworks/MolProps/Datasets/'
output_file = 'Dataset Statistics.csv'

names   =         ['Carroll_Chen_Pan_Saldana_Gelest_merge_1.csv', 'Carroll_Chen_Pan_Saldana_V1.csv', 'Carroll_Saldana_V1.csv', 'Carroll11.csv', 'Chen_Pan_Gelest_merge_1.csv', 'chen14.csv',   'gelest_dataset1.csv', 'pan12.csv',      'pan12_chen14.csv', 'pan12_chen14_SMILES_Cln.csv', 'Saldana11(fuels).csv']
id_cols  =        ['name',                                        'compound',                        'compound',               'Compound',      'name',                        'Compond Name', 'compound',            'Compounds Name', 'name',             'compounds',                   'Name']
data_cols=        ['data',                                        'flashPoint',                      'flashPoint',             'TB(K)',         'data',                        'Observed',     'flashPoint',          'Observed',       'data',             'flashPoint',                  'FP Exp.']
data_processing = [nothing,                                       nothing,                           nothing,                  nothing,         nothing,                       C2K,            nothing,               C2K,              C2K,                C2K,                          nothing]

#### Everything below here is the same for all datasets ####

files    = np.transpose([names, id_cols, data_cols, data_processing])

stats = pd.DataFrame(columns=['Dataset', 'Mean', 'Std Dev', 'Min', 'Max', 'Notes'])
for file in files:
	try:
		#file[0]=filename, file[1] = data id, file[2] = data value, file[3] = how to process data
		path = base+file[0]
		csv = pd.read_csv(path, usecols=file[1:3])

		#Renames columns to a single standard
		csv.columns=(['name', 'data'])
		#processes data (converts C to K, probably)
		csv['data'] = file[3](csv['data'])

		data = csv['data']
		mean = np.mean(data)
		std = np.std(data)
		least = np.min(data)
		most = np.max(data)
		out = {'Dataset':file[0], 'Mean':mean, 'Std Dev':std, 'Min':least, 'Max':most}
		stats = stats.append(out, ignore_index=True)
	except:
		print('something wrong happened in ' + file[0])

stats.to_csv(base + output_file)