import pyspark
from pyspark.sql.functions import *

def clean_amazon_data(spark, amazon_source):
	df = spark.read.parquet(amazon_source)

	df=df.dropDuplicates(['review_id'])
	df=df.filter(df.marketplace=='US')
	df=df.dropna(how='any', subset=['customer_id','review_id','product_id','star_rating','review_date'])
	df=df.drop('product_parent', 'total_votes', 'vine', 'marketplace', 'year', 'review_id')
	df=df.withColumn('verified_purchase', df.verified_purchase.cast('boolean'))
	df=df.fillna(False,subset=['verified_purchase'])
	df=df.fillna(0,subset=['helpful_votes'])

	return df

def clean_ucsd_data(spark, meta_source, review_source):
	# META_DATA

	all_meta_df=spark.read.json(meta_source)

	filtered_meta_df=all_meta_df.select('asin','main_cat','title')
	filtered_meta_df=filtered_meta_df.dropDuplicates(['asin'])

	# rename columns
	filtered_meta_df=filtered_meta_df.withColumnRenamed('asin','product_id')
	filtered_meta_df=filtered_meta_df.withColumnRenamed('main_cat','product_category')
	filtered_meta_df=filtered_meta_df.withColumnRenamed('title','product_title')

	# drop NA
	filtered_meta_df=filtered_meta_df.dropna(how='any')

	# filter unformatted rows, i.e. some may still contain html style content
	filtered_meta_df=filtered_meta_df.filter(~filtered_meta_df.product_id.contains('getTime()'))
	filtered_meta_df=filtered_meta_df.filter(~filtered_meta_df.product_category.contains('getTime()'))
	filtered_meta_df=filtered_meta_df.filter(~filtered_meta_df.product_title.contains('getTime()'))

	# REVIEW_DATA

	ucsd_review_df=spark.read.json(review_source)

	# drop columns, drop duplicates
	ucsd_review_df=ucsd_review_df.drop('image','reviewTime','reviewerName','style')
	ucsd_review_df=ucsd_review_df.dropDuplicates()

	# rename columns
	ucsd_review_df=ucsd_review_df.withColumnRenamed('asin','product_id')
	ucsd_review_df=ucsd_review_df.withColumnRenamed('overall','star_rating')
	ucsd_review_df=ucsd_review_df.withColumnRenamed('reviewText','review_body')
	ucsd_review_df=ucsd_review_df.withColumnRenamed('reviewerID','customer_id')
	ucsd_review_df=ucsd_review_df.withColumnRenamed('summary','review_headline')
	ucsd_review_df=ucsd_review_df.withColumnRenamed('verified','verified_purchase')
	ucsd_review_df=ucsd_review_df.withColumnRenamed('vote','helpful_votes')

	# drop NA
	ucsd_review_df=ucsd_review_df.dropna(how='any', subset=['customer_id','product_id','star_rating','unixReviewTime','review_body'])

	# convert dtype
	ucsd_review_df=ucsd_review_df.withColumn('star_rating', ucsd_review_df.star_rating.cast('INT'))
	ucsd_review_df=ucsd_review_df.withColumn('helpful_votes', ucsd_review_df.helpful_votes.cast('INT'))

	# fill na
	ucsd_review_df=ucsd_review_df.fillna(0, subset=['helpful_votes'])
	ucsd_review_df=ucsd_review_df.fillna('No title', subset=['review_headline'])
	ucsd_review_df=ucsd_review_df.fillna(False, subset=['verified_purchase'])

	# filter unformatted rows, i.e. some may still contain html style content
	ucsd_review_df=ucsd_review_df.filter(~ucsd_review_df.product_id.contains('getTime()'))
	ucsd_review_df=ucsd_review_df.filter(~ucsd_review_df.review_body.contains('getTime()'))
	ucsd_review_df=ucsd_review_df.filter(~ucsd_review_df.customer_id.contains('getTime()'))
	ucsd_review_df=ucsd_review_df.filter(~ucsd_review_df.review_headline.contains('getTime()'))

	# unixtime -> date
	ucsd_review_df=ucsd_review_df.withColumn('review_date', from_unixtime('unixReviewTime',format='yyyy-MM-dd'))
	ucsd_review_df=ucsd_review_df.withColumn('review_date', to_date('review_date',format='yyyy-MM-dd'))
	ucsd_review_df=ucsd_review_df.drop('unixReviewTime')

	# JOIN
	ucsd_df=ucsd_review_df.join(filtered_meta_df, 'product_id', 'inner')

	return ucsd_df