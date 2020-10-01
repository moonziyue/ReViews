import time
import pyspark
from pyspark.sql import SparkSession

import clean_data

# connect to Spark
def connect_to_spark(app=None, memory='6gb')
	spark = SparkSession\
			.builder
			.appName(app)\
			.master('spark://ec2-3-87-157-15.compute-1.amazonaws.com:7077')\
			.config('spark.executor.memory', memory)\
			.getOrCreate()

def main():
	start_time=time.time()
	connect_to_spark('ReView', '6gb')

	# clean data
	amazon_df=clean_data.clean_amazon_data()
	ucsd_df=clean_ucsd_data()
	print('Data cleaned in:', time.time()-start_time, 'secends.')

if __name__ == '__main__':
	main()