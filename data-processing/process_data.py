from pyspark.sql.functions import *

def aggregate_customer(df):
	# agg star_rating
	customer_rating_df=df.groupby('customer_id').pivot('star_rating').count().fillna(0)

	customer_rating_df=customer_rating_df.withColumnRenamed('1','one_star')
	customer_rating_df=customer_rating_df.withColumnRenamed('2','two_star')
	customer_rating_df=customer_rating_df.withColumnRenamed('3','three_star')
	customer_rating_df=customer_rating_df.withColumnRenamed('4','four_star')
	customer_rating_df=customer_rating_df.withColumnRenamed('5','five_star')
	# customer_rating_df.show()

	# agg other info
	stats_df=df.groupBy('customer_id').agg(
    count('review_id').alias('count'),
    sum(when(col('verified_purchase') == True, 1)).alias('verified_purchase'),
    sum('helpful_votes').alias('helpful_votes')
	)
	# stats_df.show()

	# join
	customer_summary_df=customer_rating_df.join(stats_df, 'customer_id', 'inner')
	# customer_summary_df.show()

	return customer_summary_df

def aggregate_product(df):
	product_summary_df=df.groupby('customer_id','review_date').agg(
		avg('star_rating').alias('avg_rating'),
		count('star_rating').alias('count')
		)

	return product_summary_df