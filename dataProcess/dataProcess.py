import os

from pyspark.sql import SparkSession, types
import pyspark.sql.functions as F
import sys
import json
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+


# add more functions as necessary
def main(inputs, output):
    views_schema = types.StructType([
        types.StructField('listing_id', types.StringType()),
        types.StructField('id', types.StringType()),
        types.StructField('date', types.DateType()),
        types.StructField('reviewer_id', types.LongType()),
        types.StructField('reviewer_name', types.StringType()),
        types.StructField('comments', types.StringType()),
    ])

    listings_schema = types.StructType([
        types.StructField('id', types.IntegerType(), False),
        types.StructField('listing_url', types.StringType(), False),
        types.StructField('scrape_id', types.LongType(), False),
        types.StructField('last_scraped', types.DateType(), False),
        types.StructField('source', types.StringType(), False),
        types.StructField('name', types.StringType(), False),
        types.StructField('description', types.StringType(), False),
        types.StructField('neighborhood_overview', types.StringType(), False),
        types.StructField('picture_url', types.StringType(), False),
        types.StructField('host_id', types.LongType(), False),
        types.StructField('host_url', types.StringType(), False),
        types.StructField('host_name', types.StringType(), False),
        types.StructField('host_since', types.DateType(), False),
        types.StructField('host_location', types.StringType(), False),
        types.StructField('host_about', types.StringType(), False),
        types.StructField('host_response_time', types.StringType(), False),
        types.StructField('host_response_rate', types.StringType(), False),
        types.StructField('host_acceptance_rate', types.StringType(), False),
        types.StructField('host_is_superhost', types.StringType(), False),
        types.StructField('host_thumbnail_url', types.StringType(), False),
        types.StructField('host_picture_url', types.StringType(), False),
        types.StructField('host_neighbourhood', types.StringType(), False),
        types.StructField('host_listings_count', types.IntegerType(), False),
        types.StructField('host_total_listings_count', types.IntegerType(), False),
        types.StructField('host_verifications', types.StringType(), False),
        types.StructField('host_has_profile_pic', types.StringType(), False),
        types.StructField('host_identity_verified', types.StringType(), False),
        types.StructField('neighbourhood', types.StringType(), False),
        types.StructField('neighbourhood_cleansed', types.StringType(), False),
        types.StructField('neighbourhood_group_cleansed', types.StringType(), False),
        types.StructField('latitude', types.DoubleType(), False),
        types.StructField('longitude', types.DoubleType(), False),
        types.StructField('property_type', types.StringType(), False),
        types.StructField('room_type', types.StringType(), False),
        types.StructField('accommodates', types.IntegerType(), False),
        types.StructField('bathrooms', types.IntegerType(), False),
        types.StructField('bathrooms_text', types.StringType(), False),
        types.StructField('bedrooms', types.IntegerType(), False),
        types.StructField('beds', types.IntegerType(), False),
        types.StructField('amenities', types.StringType(), False),
        types.StructField('price', types.StringType(), False),
        types.StructField('minimum_nights', types.IntegerType(), False),
        types.StructField('maximum_nights', types.IntegerType(), False),
        types.StructField('minimum_minimum_nights', types.IntegerType(), False),
        types.StructField('maximum_minimum_nights', types.IntegerType(), False),
        types.StructField('minimum_maximum_nights', types.IntegerType(), False),
        types.StructField('maximum_maximum_nights', types.IntegerType(), False),
        types.StructField('minimum_nights_avg_ntm', types.DoubleType(), False),
        types.StructField('maximum_nights_avg_ntm', types.DoubleType(), False),
        types.StructField('calendar_updated', types.StringType(), False),
        types.StructField('has_availability', types.StringType(), False),
        types.StructField('availability_30', types.IntegerType(), False),
        types.StructField('availability_60', types.IntegerType(), False),
        types.StructField('availability_90', types.IntegerType(), False),
        types.StructField('availability_365', types.IntegerType(), False),
        types.StructField('calendar_last_scraped', types.DateType(), False),
        types.StructField('number_of_reviews', types.IntegerType(), False),
        types.StructField('number_of_reviews_ltm', types.IntegerType(), False),
        types.StructField('number_of_reviews_l30d', types.IntegerType(), False),
        types.StructField('first_review', types.DateType(), False),
        types.StructField('last_review', types.DateType(), False),
        types.StructField('review_scores_rating', types.DoubleType(), False),
        types.StructField('review_scores_accuracy', types.DoubleType(), False),
        types.StructField('review_scores_cleanliness', types.DoubleType(), False),
        types.StructField('review_scores_checkin', types.DoubleType(), False),
        types.StructField('review_scores_communication', types.DoubleType(), False),
        types.StructField('review_scores_location', types.DoubleType(), False),
        types.StructField('review_scores_value', types.DoubleType(), False),
        types.StructField('license', types.StringType(), False),
        types.StructField('instant_bookable', types.StringType(), False),
        types.StructField('calculated_host_listings_count', types.IntegerType(), False),
        types.StructField('calculated_host_listings_count_entire_homes', types.IntegerType(), False),
        types.StructField('calculated_host_listings_count_private_rooms', types.IntegerType(), False),
        types.StructField('calculated_host_listings_count_shared_rooms', types.IntegerType(), False),
        types.StructField('reviews_per_month', types.DoubleType(), False),
    ])

    # # main logic starts here
    # # hardcode path for listings & reviews
    # # dire path template: COUNTRY / CITY / listings.csv.gz  reviews.csv.gz
    listings_file = 'listings.csv.gz'
    reviews_file = 'reviews.csv.gz'
    data_listings = []
    data_views = []

    listings_dir = output + '/' + 'listings'
    reviews_dir = output + '/' + 'reviews'

    for country in next(os.walk(inputs))[1]:
        for city in next(os.walk(inputs + '/' + country))[1]:
            # listings process
            listings_path = inputs + '/' + country + '/' + city + '/' + listings_file
            listings = spark.read.csv(listings_path, schema=listings_schema, header=True, multiLine=True, quote='"',
                                    escape='"')

            # 1 add country city column
            listings = listings.withColumn('country', F.lit(country))
            listings = listings.withColumn('city', F.lit(city))
            # listings process
            # 1 replace null
            listings = listings.fillna(
                {'license': '',  'beds': 0, 'bedrooms': 0,
                 'bathrooms_text': '', 'name': ''})
            # columns we need
            columns = ['id', 'listing_url',  'host_since', 'host_response_time', 'host_acceptance_rate',
                       'host_is_superhost', 'host_listings_count', 'host_identity_verified',
                       'neighbourhood_cleansed', 'latitude', 'longitude', 'room_type',
                       'accommodates', 'bathrooms_text', 'bedrooms', 'beds', 'price', 'license',
                       'number_of_reviews', 'review_scores_rating', 'review_scores_accuracy', 'review_scores_cleanliness',
                       'review_scores_checkin', 'review_scores_communication', 'review_scores_location',
                       'review_scores_value', 'reviews_per_month',
                       'country', 'city']
            listings = listings.select(*columns)
            # price column $xx to xx
            listings = listings.withColumn('price', F.regexp_extract('price', '\$(.*)$', 1).cast('double'))
            # change license to true & false
            listings = listings.withColumn('license', F.when(listings['license'] == '', '0').
                                           otherwise('1'))
            # change bathrooms_text to number
            listings = listings.withColumn('tmp', F.split(listings['bathrooms_text'], '\s+')).\
                                withColumn('bathrooms', F.col('tmp').getItem(0).cast('double')).\
                                withColumn('bathrooms', F.when(F.col('bathrooms').isNotNull(), F.col('bathrooms')).
                                           otherwise(0)).\
                                drop('tmp', 'bathrooms_text')

            # reviews process
            reviews_path = inputs + '/' + country + '/' + city + '/' + reviews_file
            reviews = spark.read.csv(reviews_path, schema=views_schema, header=True, multiLine=True, quote='"',
                                    escape='"')
            reviews = reviews.withColumn('country', F.lit(country))
            reviews = reviews.withColumn('city', F.lit(city))
            # reviews
            # 1 replace null comments to ''
            reviews = reviews.fillna('')
            # 2 replace n/a comments to ''
            reviews = reviews.withColumn('comments', F.when(reviews['comments'] == 'n/a', '').
                                         otherwise(reviews['comments']))
            # write
            listings.write.parquet(listings_dir, mode="append", compression="snappy")
            reviews.write.parquet(reviews_dir, mode="append", compression="snappy")

    # schema after data process: only for column check
    # views_schema = types.StructType([
    #     types.StructField('listing_id', types.StringType()),
    #     types.StructField('id', types.StringType()),
    #     types.StructField('date', types.DateType()),
    #     types.StructField('reviewer_id', types.LongType()),
    #     types.StructField('reviewer_name', types.StringType()),
    #     types.StructField('comments', types.StringType()),
    #     types.StructField('country', types.StringType()),
    #     types.StructField('city', types.StringType()),
    # ])
    #
    # listings_schema = types.StructType([
    #     types.StructField('id', types.IntegerType(), False),
    #     types.StructField('listing_url', types.StringType(), False),
    #     types.StructField('neighbourhood_cleansed', types.StringType(), False),
    #     types.StructField('latitude', types.DoubleType(), False),
    #     types.StructField('longitude', types.DoubleType(), False),
    #     types.StructField('room_type', types.StringType(), False),
    #     types.StructField('accommodates', types.IntegerType(), False),
    #     types.StructField('bedrooms', types.IntegerType(), False),
    #     types.StructField('beds', types.IntegerType(), False),
    #     types.StructField('price', types.DoubleType(), False),
    #     types.StructField('license', types.StringType(), False),
    #     types.StructField('number_of_reviews', types.IntegerType(), False),
    #     types.StructField('review_scores_rating', types.DoubleType(), False),
    #     types.StructField('review_scores_accuracy', types.DoubleType(), False),
    #     types.StructField('review_scores_cleanliness', types.DoubleType(), False),
    #     types.StructField('review_scores_checkin', types.DoubleType(), False),
    #     types.StructField('review_scores_communication', types.DoubleType(), False),
    #     types.StructField('review_scores_location', types.DoubleType(), False),
    #     types.StructField('review_scores_value', types.DoubleType(), False),
    #     types.StructField('reviews_per_month', types.DoubleType(), False),
    #     types.StructField('country', types.StringType()),
    #     types.StructField('city', types.StringType()),
    #     types.StructField('bathrooms', types.DoubleType()),
    # ])
    # l = spark.read.parquet(listings_dir)
    # r = spark.read.parquet(reviews_dir)
    # l.show()
    # r.show()


if __name__ == '__main__':
    # raw data path
    ## rawData
    inputs = sys.argv[1]
    # processed data path
    ## data
    output = sys.argv[2]
    spark = SparkSession.builder.appName('data process').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
