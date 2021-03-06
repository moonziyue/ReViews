from pyspark.sql.functions import *

def aggregate_customer(df):

	df=df.select('customer_id','star_rating','verified_purchase','helpful_votes')
	df=df.repartition('customer_id')

	customer_summary_df=df.groupBy('customer_id').agg(
		sum(when(col('star_rating') == 1, 1).otherwise(0)).alias('one_star'),
		sum(when(col('star_rating') == 2, 1).otherwise(0)).alias('two_star'),
		sum(when(col('star_rating') == 3, 1).otherwise(0)).alias('three_star'),
		sum(when(col('star_rating') == 4, 1).otherwise(0)).alias('four_star'),
		sum(when(col('star_rating') == 5, 1).otherwise(0)).alias('five_star'),
		count('star_rating').alias('total_purchase'),
		sum(when(col('verified_purchase') == True, 1).otherwise(0)).alias('verified_purchase'),
		sum('helpful_votes').alias('helpful_votes')
    )

	customer_summary_df=customer_summary_df.withColumn('one_star', customer_summary_df.one_star.cast('INT'))
	customer_summary_df=customer_summary_df.withColumn('two_star', customer_summary_df.two_star.cast('INT'))
	customer_summary_df=customer_summary_df.withColumn('three_star', customer_summary_df.three_star.cast('INT'))
	customer_summary_df=customer_summary_df.withColumn('four_star', customer_summary_df.four_star.cast('INT'))
	customer_summary_df=customer_summary_df.withColumn('five_star', customer_summary_df.five_star.cast('INT'))
	customer_summary_df=customer_summary_df.withColumn('total_purchase', customer_summary_df.total_purchase.cast('INT'))
	customer_summary_df=customer_summary_df.withColumn('verified_purchase', customer_summary_df.verified_purchase.cast('INT'))
	customer_summary_df=customer_summary_df.withColumn('helpful_votes', customer_summary_df.helpful_votes.cast('INT'))

	return customer_summary_df

def aggregate_product(df):
	
	df=df.select('product_id','review_date','star_rating')
	df=df.repartition('product_id')
	
	product_summary_df=df.groupby('product_id','review_date').agg(
		avg('star_rating').alias('avg_rating'),
		count('star_rating').alias('total_reviews')
		)
	product_summary_df=product_summary_df.withColumn('total_reviews',product_summary_df.total_reviews.cast('INT'))

	return product_summary_df

def union_df(df1, df2):
	
	df=df1.unionByName(df2).distinct()
	return df

def get_review(df):
	
	df=df.select('customer_id','product_id','product_title','star_rating','verified_purchase','review_body','review_date')
	return df





