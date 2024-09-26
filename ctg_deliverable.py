import csv
import glob 
from concurrent.futures import ThreadPoolExecutor
import os


def read_csv_file(file):
    """Helper function to read a single CSV file."""
    data = []
    try:
        with open(file, newline='') as f:
            reader = csv.DictReader(f)
            data.extend(list(reader))
    except Exception as e:
        print(f"Error reading {file}: {e}")
    return data


def loadData(path):
	#specifiy directory of timetick data
	data_directory  =  path

	#load all csv files into one list
	csv_files = glob.glob(f'{data_directory}*.csv')


	#initalize list to hold all tick data
	all_data = []

	#initialize threadpoolexecutor object
	executor = ThreadPoolExecutor(max_workers = 8)

	futures = []
	for file in csv_files:
		#load each csv processing task as a seperate future to be executed asyncrhonously 
		future = executor.submit(read_csv_file,file)
		futures.append(future)

	#append each processed csv result to alldata
	for future in futures:
		try:
			#result only gets populated if current future status is completed
			result = future.result()
			all_data.extend(result)
		except Exception as e:
			print(f"could not process file")

	#abort threadpool after use
	executor.shutdown()

	return all_data

	



def writeCsv(data, output_file_path):

	headers = data[0].keys();
	with open(output_file_path, mode = "w", newline = '') as file:
		writer = csv.DictWriter(file, fieldnames = headers)
		writer.writeheader()
		writer.writerows(data)


def main():
	#cpu_count = os.cpu_count()
	#print(f"Number of CPUs: {cpu_count}")

	allData = loadData('data/')
	writeCsv(allData, 'tickdata.csv')
	
if __name__ == "__main__":
	main()