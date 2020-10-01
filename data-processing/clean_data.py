import pyspark
from pyspark.sql.functions import *

def clean_amazon_data(spark):
	df = spark.read.parquet("s3a://amazon-reviews-pds/parquet")

	# original: 1,6079,6570
	# no-duplicates: 1,5386,5404
	# US-only: 1,5096,2278
	df=df.dropDuplicates(['review_id'])
	df=df.filter(df.marketplace=='US')
	df=df.dropna(how='any', subset=['customer_id','review_id','product_id','star_rating','review_date'])
	df=df.drop('product_parent', 'total_votes', 'vine', 'marketplace', 'year')
	df=df.withColumn('verified_purchase', df.verified_purchase.cast('boolean'))
	# df.printSchema()
	print('amazon done')

	return df

def clean_ucsd_data(spark):
	# META_DATA

	all_meta_df=spark.read.json('s3a://amazonreviewsdataset/USCD_RAW/UCSD_JSON_META.json')
	# all_meta_df.printSchema()

	# raw: 15023059
	# drop-duplicates: 14741571
	filtered_meta_df=all_meta_df.select('asin','main_cat','title')
	filtered_meta_df=filtered_meta_df.dropDuplicates(['asin'])

	# rename columns
	filtered_meta_df=filtered_meta_df.withColumnRenamed('asin','product_id')
	filtered_meta_df=filtered_meta_df.withColumnRenamed('main_cat','product_category')
	filtered_meta_df=filtered_meta_df.withColumnRenamed('title','product_title')

	# filter unformatted rows, i.e. some may still contain html style content
	filtered_meta_df=filtered_meta_df.filter(~filtered_meta_df.product_id.contains('getTime()'))
	filtered_meta_df=filtered_meta_df.filter(~filtered_meta_df.product_category.contains('getTime()'))
	filtered_meta_df=filtered_meta_df.filter(~filtered_meta_df.product_title.contains('getTime()'))

	# filtered_meta_df.printSchema()

	# REVIEW_DATA

	ucsd_review_df=spark.read.json('s3a://amazonreviewsdataset/USCD_RAW/UCSD_JSON.json')
	# ucsd_review_df.printSchema()

	# drop columns, drop duplicates
	ucsd_review_df=ucsd_review_df.drop('image','reviewTime','reviewerName','style')
	ucsd_review_df=ucsd_review_df.dropDuplicates()
	# ucsd_review_df.printSchema()

	# rename columns
	ucsd_review_df=ucsd_review_df.withColumnRenamed('asin','product_id')
	ucsd_review_df=ucsd_review_df.withColumnRenamed('overall','star_rating')
	ucsd_review_df=ucsd_review_df.withColumnRenamed('reviewText','review_body')
	ucsd_review_df=ucsd_review_df.withColumnRenamed('reviewerID','customer_id')
	ucsd_review_df=ucsd_review_df.withColumnRenamed('summary','review_headline')
	ucsd_review_df=ucsd_review_df.withColumnRenamed('verified','verified_purchase')
	ucsd_review_df=ucsd_review_df.withColumnRenamed('vote','helpful_votes')

	# convert dtype
	ucsd_review_df=ucsd_review_df.withColumn('star_rating', ucsd_review_df.star_rating.cast('INT'))
	ucsd_review_df=ucsd_review_df.withColumn('helpful_votes', ucsd_review_df.helpful_votes.cast('INT'))

	# unixtime -> date
	ucsd_review_df=ucsd_review_df.withColumn('review_date', from_unixtime('unixReviewTime',format='yyyy-MM-dd'))
	ucsd_review_df=ucsd_review_df.withColumn('review_date', to_date('review_date',format='yyyy-MM-dd'))
	ucsd_review_df=ucsd_review_df.drop('unixReviewTime')

	# drop NA
	ucsd_review_df=ucsd_review_df.dropna(how='any', subset=['customer_id','product_id','star_rating','review_date'])

	# filter unformatted rows, i.e. some may still contain html style content
	ucsd_review_df=ucsd_review_df.filter(~ucsd_review_df.product_id.contains('getTime()'))
	ucsd_review_df=ucsd_review_df.filter(~ucsd_review_df.review_body.contains('getTime()'))
	ucsd_review_df=ucsd_review_df.filter(~ucsd_review_df.customer_id.contains('getTime()'))
	ucsd_review_df=ucsd_review_df.filter(~ucsd_review_df.review_headline.contains('getTime()'))

	# ucsd_review_df.printSchema()

	# JOIN
	ucsd_df=ucsd_review_df.join(filtered_meta_df, 'product_id', 'inner')

	print('ucsd done')

	return ucsd_df