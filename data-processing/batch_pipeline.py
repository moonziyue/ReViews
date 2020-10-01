import time
import pyspark
from pyspark.sql import SparkSession

import clean_data

# connect to Spark
def connect_to_spark(app=None, memory='6gb'):
	_spark = SparkSession\
			.builder\
			.appName(app)\
			.master('spark://ec2-3-87-157-15.compute-1.amazonaws.com:7077')\
			.config('spark.executor.memory', memory)\
			.getOrCreate()
	return _spark

def main():
	start_time=time.time()
	
	# create spark sesstion
	spark=connect_to_spark('ReView', '6gb')

	# clean data
	amazon_source='s3a://amazon-reviews-pds/parquet'
	meta_source='s3a://amazonreviewsdataset/USCD_RAW/UCSD_JSON_META.json'
	review_source='s3a://amazonreviewsdataset/USCD_RAW/UCSD_JSON.json'

	amazon_df=clean_data.clean_amazon_data(spark, amazon_source)
	ucsd_df=clean_data.clean_ucsd_data(spark, meta_source, review_source)
	
	print('Data cleaned in:', time.time()-start_time, 'secends.')



if __name__ == '__main__':
	main()