import os
import csv
import glob
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, time, timedelta

class CTGDeliverable:
    def __init__(self, data_path, output_file, interval_str, max_workers=8):
        self.data_path = data_path
        self.output_file = output_file
        self.interval_str = interval_str
        self.max_workers = max_workers
        self.start_time = time(9, 30)
        self.end_time = time(16, 0)

    def _read_csv_file(self, file):
        """Helper function to read a single CSV file."""
        data = []
        try:
            with open(file, newline='') as f:
                reader = csv.DictReader(f)
                data.extend(list(reader))
        except Exception as e:

            raise ValueError(f"Error reading {file}: {e}")
        return data

    def load_data(self):
    	 #specifiy directory of timetick data
    	data_directory  =  self.data_path

    	#load all csv files into one list
    	csv_files = glob.glob(f'{self.data_path}*.csv')

    	#initalize list to hold dictionaries of tick data (keys: headers, values = values)
    	all_data = []
    	executor = ThreadPoolExecutor(max_workers = 8)
    	futures = []


    	for file in csv_files:
    		#load each csv processing task as a seperate future to be executed asyncrhonously 

    		future = executor.submit(self._read_csv_file,file)
    		futures.append(future)

    	#append each processed csv result to alldata
    	for future in futures:
    		try:
    			#result only gets populated if current future status is completed
    			result = future.result()
    			all_data.extend(result)
    		except Exception as e:
    			raise ValueError("Could not process file")


		#abort threadpool after use

    	executor.shutdown()
    	return all_data


    def filter_data(self, data):

        """Filter data by removing duplicates and invalid rows."""
        filtered_data = []
        for row in data:

            timestamp = datetime.strptime(row['Timestamp'], "%Y-%m-%d %H:%M:%S.%f")

            #boolean to check for valid time range
            is_within_valid_time = self.start_time <= timestamp.time() <= self.end_time

            if (row['Timestamp'] != '' and row['Price'] != '' and row['Size'] != '' 
                and float(row['Price']) >= 0 and float(row['Size']) >= 0 
                and is_within_valid_time and row['Timestamp']):

                filtered_data.append(row)
        return filtered_data

    def _parse_interval(self):
    	time_map = {'d': 86400, 'h': 3600, 'm': 60, 's': 1}
    	total_seconds = 0

    	num = ''
    	if not self.interval_str:  # Check for empty string
    		raise ValueError("Interval string cannot be empty.")


		#Iterate through each character in the interval string
    	for char in self.interval_str:
    		if char.isdigit():
    			num += char
    		elif char in time_map:
    			if num == '':  # Check if there is no number before the unit
    				raise ValueError(f"Time unit '{char}' is missing a preceding number.")
    				total_seconds += int(num) * time_map[char]
    				num = ''
    		else:
    			raise ValueError(f"Invalid character '{char}' found in interval string.")

    		# If there's any leftover number without a time unit
    		if num != '':	   
    			raise ValueError("Incomplete interval string, number without time unit.")
    	return total_seconds

    def _process_interval_data(self, data):
        """Process data for a single OHLCV bar."""
        open_price = float(data[0]['Price'])
        close_price = float(data[-1]['Price'])
        high_price = max(float(row['Price']) for row in data)
        low_price = min(float(row['Price']) for row in data)
        volume = sum(int(row['Size']) for row in data)
        timestamp = data[0]['Timestamp']
        return {'Timestamp': timestamp, 'Open': open_price, 'High': high_price, 'Low': low_price, 'Close': close_price, 'Volume': volume}

    def generate_ohlcv(self, data):
        """Generate OHLCV data from filtered ticks."""
        ohlcv = []
        interval_seconds = self._parse_interval()
        current_interval_start = None
        current_interval_data = []


        # Initialize the first interval
        for row in data:
            dt = datetime.strptime(row['Timestamp'], '%Y-%m-%d %H:%M:%S.%f')

        # Initialize the first interval

            if current_interval_start is None:
                current_interval_start = dt
                current_interval_data.append(row)
                continue

 			# If the current row belongs to the next interval than we will add the current data to OHLCV and move on
     
            if (dt - current_interval_start).total_seconds() >= interval_seconds:

            	# Process and append the current interval's OHLCV data

                ohlcv.append(self._process_interval_data(current_interval_data))
                current_interval_start = dt
                current_interval_data = [row]
            else:

                current_interval_data.append(row)

        # Process any remaining data for the final interval
        if current_interval_data:
            ohlcv.append(self._process_interval_data(current_interval_data))

        return ohlcv

    def write_to_csv(self, ohlcv_data):
        """Write the OHLCV data to a CSV file."""
        with open(self.output_file, 'w', newline='') as csvfile:
            fieldnames = ['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(ohlcv_data)


def get_time_range_from_user():
    # Define the valid time range
    valid_start_time = datetime.strptime("2023-09-20 09:30:00", "%Y-%m-%d %H:%M:%S")
    valid_end_time = datetime.strptime("2023-09-20 16:00:00", "%Y-%m-%d %H:%M:%S")

    # Get user input for start and end times
    start_time_str = input("Enter the start time (YYYY-MM-DD HH:MM): ")
    end_time_str = input("Enter the end time (YYYY-MM-DD HH:MM): ")

    # Convert user input to datetime objects
    try:
        start_time = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M")
        end_time = datetime.strptime(end_time_str, "%Y-%m-%d %H:%M")
    except ValueError:
        print("Invalid datetime format. Please use YYYY-MM-DD HH:MM.")
        return None, None  # Return None if there's an error

    # Validate the input time range
    if not (valid_start_time <= start_time <= valid_end_time):
        print(f"Start time must be between {valid_start_time} and {valid_end_time}.")
        return None, None

    if not (valid_start_time <= end_time <= valid_end_time):
        print(f"End time must be between {valid_start_time} and {valid_end_time}.")
        return None, None

    return start_time, end_time  # Return valid datetime objects

   

def main():

	
	interval_str = input("Enter a time interval: ")

	start_time, end_time = get_time_range_from_user()
	if start_time is None or end_time is None:

		return  # Exit if the time range is invalid


	deliverable = CTGDeliverable(data_path = "data",output_file = "output_file.csv", interval_str = interval_str)


	deliverable._parse_interval()

	
	print("Loading data...")
	data = deliverable.load_data()

	print("Cleaning data...")
	cleaned_data = deliverable.filter_data(data)

	print("Generating OHLCV data...")
	ohlcv_data = deliverable.generate_ohlcv(cleaned_data)

	print("Writing OHLCV data to CSV...")
	deliverable.write_to_csv(ohlcv_data)

	print("Process completed successfully!")

if __name__ == "__main__":
	main()

    
