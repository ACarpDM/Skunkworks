import numpy as np
import pandas as pd

#### These things change depending on the dataset ####
def nothing(val):
	return val
def C2K(celsius):
	return celsius+273

base = 'C:/Users/Jimbo/Documents/Coleg/Skunkworks/MolProps/Datasets/'

names   = ['Carroll11_fixed.csv', 'Chen_2014_dataset.csv', 'pan12.csv',		'Saldana11_simplified.csv', 'gelest_dataset1.csv']
id_cols  = ['Compound',			  'Compond Name',		   'Compounds Name','Name',						'compound']
data_cols= ['TFP(K)Ref.',		  'Observed',			   'Observed',		'FP Exp.',					'flashPoint']
data_processing = [nothing, 	  nothing, 			 	   C2K,				nothing,					C2K]


output_file = 'Carroll_Chen_Pan_Saldana_Gelest_merge_1.csv'
errata_file = 'errata.csv'
#### Everything below here is the same for all datasets ####

files    = np.transpose([names, id_cols, data_cols, data_processing])

combined = pd.DataFrame(columns=['name', 'data', 'source'])
for file in files:
	#file[0]=filename, file[1] = data name, file[2] = data value, file[3] = how to process data
	path = base+file[0]
	csv = pd.read_csv(path, usecols=file[1:3])

	#labels entries w/ the source datafile
	source = pd.Series(file[0], range(len(csv)), name='source')
	csv = csv.join(source)

	#Renames columns to a single standard
	csv.columns=(['name', 'data', 'source'])
	csv['data'] = file[3](csv['data'])
	combined = combined.append(csv, ignore_index=True, sort=True)

#sorts values
combined['name'].str.strip()
combined.sort_values(by=['name'])

names = combined['name'].drop_duplicates()
print('merging {:d} values'.format(len(names)))


output = pd.DataFrame(columns=['name', 'data'])
errata = pd.DataFrame(columns=['name', 'data', 'source'])

#Drops all rows containing NaN values, and adds the drops values to errata
errata = errata.append(combined[combined.isnull().any(axis=1)], sort=True)
combined.dropna(axis=0, how='any', inplace=True)
#searches for duplicates
for name in names.iteritems():
	name = name[1]
	#dups is a DataFrame containing all values with 'name'
	dups = combined[combined['name']==name]
	vals = dups['data'].values;
	if(len(vals)>1 ):
		if(np.std(vals)>3):
			errata = errata.append(dups, sort=True)
		else:
			#takes the average of the values and appends it to output
			avg = np.mean(vals)
			newVal = pd.DataFrame(data=[[name, avg, 'merged']], columns=['name', 'data', 'source'])
			output = output.append(newVal, sort=True)
	else:
		output = output.append(dups, sort=True)

output.to_csv(base+output_file)
errata.to_csv(base+errata_file)