import pyspark
from pyspark.sql.functions import *

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
# filtered_meta_df.printSchema()

# REVIEW_DATA

ucsd_review_df=spark.read.json('s3a://amazonreviewsdataset/USCD_RAW/UCSD_JSON.json')
# ucsd_review_df.printSchema()

# drop columns, drop duplicates
ucsd_review_df=ucsd_review_df.drop('image','reviewTime','reviewerName','style')
ucsd_review_df=ucsd_review_df.dropDuplicates()
ucsd_review_df.printSchema()

# rename columns
ucsd_review_df=ucsd_review_df.withColumnRenamed('asin','product_id')
ucsd_review_df=ucsd_review_df.withColumnRenamed('reviewText','review_body')
ucsd_review_df=ucsd_review_df.withColumnRenamed('reviewerID','customer_id')
ucsd_review_df=ucsd_review_df.withColumnRenamed('summary','review_headline')
ucsd_review_df=ucsd_review_df.withColumnRenamed('verified','verified_purchase')
ucsd_review_df=ucsd_review_df.withColumnRenamed('vote','helpful_votes')

# convert dtype
ucsd_review_df=ucsd_review_df.withColumn('star_rating', ucsd_review_df.overall.cast('INT'))
ucsd_review_df=ucsd_review_df.drop('overall')
# unixtime -> date
ucsd_review_df=ucsd_review_df.withColumn('review_date', from_unixtime('unixReviewTime',format='yyyy-MM-dd'))
ucsd_review_df=ucsd_review_df.withColumn('review_date', to_date('review_date',format='yyyy-MM-dd'))
ucsd_review_df=ucsd_review_df.drop('unixReviewTime')

# drop NA
ucsd_review_df=ucsd_review_df.dropna(how='any', subset=['customer_id','product_id','star_rating','review_date'])

# ucsd_review_df.printSchema()





