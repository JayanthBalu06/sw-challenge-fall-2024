import pandas as pd
import glob 
from concurrent.futures import ThreadPoolExecutor

#specifiy directory of timetick data
data_directory  = 'data/'

#load all csv files into one list
csv_files = glob.glob(f'{data_directory}*.csv')


#initialize dataframe to concatenate csv files
df_append = pd.DataFrame()



#initialize threadpoolexecutor object
executor = ThreadPoolExecutor()

#execute multple threads to read csv files
dfs = list(executor.map(pd.read_csv,csv_files))

#shutdown object
executor.shutdown()

#concatenate each data frame into one
df_append = pd.concat(dfs, ignore_index = True)


print(df_append)
