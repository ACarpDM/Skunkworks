import numpy as np
import pandas as pd
import os
import requests
from rdkit import Chem
from skunkImage import cs_compute_features, cs_set_resolution, cs_coords_to_grid, cs_check_grid_boundary

#### These things change depending on the dataset ####
def nothing(val):
    return val
def C2K(celsius):
    return celsius+273

base = os.path.expanduser("~/")
base = base + "Documents/Coleg/Skunkworks/MolProps/Datasets/"
# base = 'C:/Users/Jimbo/Documents/Coleg/Skunkworks/MolProps/Datasets/'

names   = ['Sample_Chen14.csv']#['Carroll11_fixed.csv', 'Chen_2014_dataset.csv', 'pan12.csv',     'Saldana11_simplified.csv', 'gelest_dataset1.csv']
id_cols  = ['Compond Name']#    ['Compound',            'Compond Name',          'Compounds Name','Name',                     'compound']
data_cols= ['Observed']#        ['TFP(K)Ref.',          'Observed',              'Observed',      'FP Exp.',                  'flashPoint']
data_processing = [nothing]#    [ nothing,               nothing,                 C2K,             nothing,                    C2K]


output_cols = ['Name', 'SMILES', 'Flash Point (K)', 'Source Paper(s)'] # There is a fourth 'Notes' column but that's added manually near the bottom of the code
output_file = 'Sample_Chen14_output.csv'
errata_file = 'Sample_Chen14_errata.csv'

generate_smiles = True
#### Everything below here is the same for all datasets ####

def getSmiles(compounds):
    out=[]
    for compound in compounds:
        if not generate_smiles:
            out.append('No Smiles Generated')
            continue
        smilesUrl = "https://opsin.ch.cam.ac.uk/opsin/"+compound+".smi"
        requestSmiles = requests.get(smilesUrl)
        print(compound)
        if requestSmiles.status_code == 400 or requestSmiles.status_code == 404 or len(requestSmiles.text)>250:
            print("No Smiles: " + compound)
            out.append(None)
        else:
            smiles = requestSmiles.text
            #Smiles validation, taken from https://github.com/nkrakauer/Property-Predictions/blob/2f25eb41739b174d49936f461749d6e65911ad70/dataGeneration.py
            mol = Chem.MolFromSmiles(smiles)
            mol, df_atom, df_bond, nancheckflag = cs_compute_features(mol) # compute properties
            df_atom, atomcheckflag = cs_coords_to_grid(df_atom, dim, res) # Map coors to grid
            sizecheckflag = cs_check_grid_boundary(df_atom, gridsize) # Check if outside of grid

            if nancheckflag or atomcheckflag or sizecheckflag:
                out.append(None)
            else:
                out.append(requestSmiles.text)
            print(requestSmiles.text)
    return out


files    = np.transpose([names, id_cols, data_cols, data_processing])

combined = pd.DataFrame(columns=['name', 'data', 'source'])
for file in files:
    #file[0]=filename, file[1] = data id, file[2] = data value, file[3] = how to process data
    path = base+file[0]
    csv = pd.read_csv(path, usecols=file[1:3])


    #labels entries w/ the source datafile
    source = pd.Series(file[0], range(len(csv)), name='source')
    csv = csv.join(source)

    #Generates SMILES strings
    smiles = pd.Series(getSmiles(csv[file[1]]), name='smiles')
    csv = csv.join(smiles)

    #Renames columns to a single standard
    csv.columns=(['name', 'data', 'source', 'smiles'])
    csv['data'] = file[3](csv['data'])

    combined = combined.append(csv, ignore_index=True, sort=True)

#sorts values
combined['name'].str.strip()
combined.sort_values(by=['name'])
combined = combined[['name', 'smiles', 'data', 'source']]

names = combined['name'].drop_duplicates()
print('merging {:d} values'.format(len(names)))


output = pd.DataFrame(columns=output_cols)
errata = pd.DataFrame(columns=output_cols)

#Drops all rows containing NaN values, and adds the drops values to errata
errata = errata.append(combined[combined.isnull().any(axis=1)], sort=True)
combined.dropna(axis=0, how='any', inplace=True)
#searches for duplicates
for name in names.iteritems():
    # names is a series, so name[0] is the index. name[1] is the actual name
    name = name[1]
    #dups is a DataFrame containing all values with 'name'
    dups = combined[combined['name']==name]
    vals = dups['data'].values
    if(len(vals)>1 ):
        if(np.std(vals)>3):
            dups.columns = output_cols
            errata = errata.append(dups, sort=True)
        else:
            #takes the average of the values and appends it to output
            avg = np.mean(vals)
            sources = np.sum(dups['source'].values)
            newVal = pd.DataFrame(data=[[name, avg, sources]], columns=output_cols)
            output = output.append(newVal, sort=True)
    else:
        dups.columns = output_cols
        output = output.append(dups, sort=True)

output['Notes'] = np.empty(output.shape[0], dtype=str)
output.to_csv(base+output_file)
errata.to_csv(base+errata_file)