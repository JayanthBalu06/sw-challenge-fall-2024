import pandas as pd
import glob 
from concurrent.futures import ThreadPoolExecutor

def loadData(path):
	#specifiy directory of timetick data
	data_directory  =  path

	#load all csv files into one list
	csv_files = glob.glob(f'{data_directory}*.csv')


	#initialize threadpoolexecutor object
	executor = ThreadPoolExecutor()


#handle exceptions
	try:
	#execute multple threads to read csv files
		dfs = list(executor.map(pd.read_csv,csv_files))
	except FileNotFoundError as e:
	    print(f"Error: One of the files was not found. {e}")
	except pd.errors.EmptyDataError as e:
	    print(f"Error: One of the files is empty. {e}")
	except pd.errors.ParserError as e:
	    print(f"Error: There was a problem parsing a file. {e}")
	except Exception as e:
	    print(f"An unexpected error occurred: {e}")
	finally:
	#shutdown object
		executor.shutdown()

	#concatenate each data frame into one
	df = pd.concat(dfs, ignore_index = True)

	return df


def main():
	df = loadData('data/')

	df_filtered = df[(df['Price'] >= 0) & (df['Size'] >= 0)]
	print(df_filtered.head())

	# Check the data types of each column
	print(df_filtered.dtypes)

	# Get summary statistics
	print(df_filtered.describe(include='all'))

if __name__ == "__main__":
	main()