import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext

# connect to Spark
conf = pyspark.SparkConf().setAppName('ReadReviewsFromS3').setMaster('spark://ec2-3-87-157-15.compute-1.amazonaws.com:7077')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

# read from S3
# Time: ~9s
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