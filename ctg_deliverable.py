import pandas as pd
import glob 
import os

data_directory  = 'data/'
csv_files = glob.glob(f'{data_directory}*.csv')


#initialize dataframe to concatenate csv files
df_append = pd.DataFrame()
#append all files together

#iterate through each file in array of csv files and append it to the dataframe
for file in csv_files:
	df_temp = pd.read_csv(file)
	df_append = df_append._append(df_temp, ignore_index=True)


print(df_append)