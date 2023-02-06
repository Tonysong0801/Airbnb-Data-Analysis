import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql.functions import approx_count_distinct, collect_list
from pyspark.sql.functions import collect_set, sum, avg, max, countDistinct, count
from pyspark.sql.functions import first, last, kurtosis, min, mean, skewness
from pyspark.sql.functions import stddev, stddev_samp, stddev_pop, sumDistinct, month
from pyspark.sql.functions import variance, var_samp, var_pop, when, col, expr
from pyspark.sql import SparkSession, functions, types
from pyspark.sql import functions as fn
from pyspark.sql.functions import substring


# add more functions as necessary

def main(input,output):
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
        types.StructField('price', types.DoubleType(), False),
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

    listings_input = spark.read.option("multiline", "true") \
        .option("quote", '"') \
        .option("header", "true") \
        .option("escape", "\\") \
        .option("escape", '"') \
        .csv(input, schema=listings_schema, header=True)

    listings_input.show(20)
    listings_input.printSchema()
    print(listings_input.count())

    distinct_listings = listings_input.dropDuplicates(subset=[c for c in listings_input.columns if True])
    # print(listings_input.count())

    selected = distinct_listings.select(*['id', 'neighbourhood_cleansed', 'latitude', 'longitude', 'price'])
    # selected.show()

    selected = selected.dropna(subset=['price', 'longitude', 'id', 'neighbourhood_cleansed'])
    # print(selected.count())

    max_price = selected.select(max("price")).collect()[0][0]
    # print(max_price)
    min_price = selected.select(min("price")).collect()[0][0]
    # get the lowest price

    selected = selected.orderBy(selected.price.asc())
    # selected.show()
    selected = selected.withColumn('newID', fn.monotonically_increasing_id())

    numberOfPrice = selected.select('price').count()
    # to calculate the medium
    # print(numberOfPrice)

    partition = (numberOfPrice - 1) / 3
    firstNode = partition
    secNode = partition * 2

    addedLevel = selected.withColumn('type', when(selected.newID.between(0, firstNode), 'low') \
                                     .when(selected.newID.between(firstNode, secNode), 'medium') \
                                     .when(selected.newID.between(secNode, numberOfPrice), 'high') \
                                     .otherwise(selected.price))

    # addedLevel.show()

    print(addedLevel.select('type').where(addedLevel.type == 'low').count())
    print(addedLevel.select('type').where(addedLevel.type == 'medium').count())
    print(addedLevel.select('type').where(addedLevel.type == 'high').count())
    print(addedLevel.select('type').distinct().count())

    priceWithSpace = addedLevel.select(*['neighbourhood_cleansed', 'latitude', 'longitude', 'price', 'type'])

    # priceWithSpace.show()
    # priceWithSpace.write.csv("output\priceWithSpace", mode='overwrite', header=True)
    # priceWithSpace.printSchema()

    pws2 = priceWithSpace.groupBy('neighbourhood_cleansed').agg(fn.avg('price').alias('average_price($)'))
    pws2 = pws2.filter(pws2['average_price($)'] > 100)
    pws2 = pws2.filter(pws2['average_price($)'] != 105.0)
    pws2 = pws2.filter(pws2['average_price($)'] != 200.0)
    pws2 = pws2.filter(pws2['average_price($)'] != 226.0)
    pws2 = pws2.filter(pws2['average_price($)'] < 800)
    # pws2.show()

    priceWithSpace2 = pws2.join(fn.broadcast(priceWithSpace), 'neighbourhood_cleansed')
    # priceWithSpace2.show()

    origin = priceWithSpace2.select(*['neighbourhood_cleansed', 'average_price($)', 'latitude', 'longitude'])
    origin = origin.withColumnRenamed('neighbourhood_cleansed', 'neighbourhood')
    # origin.write.csv("output\priceWithSpaceOrigin", mode='overwrite', header=True)

    result = priceWithSpace2.groupBy('neighbourhood_cleansed') \
        .agg(fn.avg('average_price($)').alias("avg_price($)"), \
             fn.avg('latitude').alias("avg_latitute"), \
             fn.avg('longitude').alias("avg_longitude"))

    result.withColumnRenamed('neighbourhood_cleansed', 'neighbourhood') \
        .write.csv(output, mode='overwrite', header=True)


if __name__ == '__main__':
    spark = SparkSession.builder.appName('priceWithspace').getOrCreate()
    assert spark.version>='3.0'
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext

    '''
    In our project, we set:
    input= "YourLocalComputer\\listings_detailed\\listings(1,2,3,4,5,6,7).csv"
    output= "../analysis_results/priceWithSpace(1,2,3,4,5,6,7)"
    '''

    input = sys.argv[1]
    output = sys.argv[2]
    main(input,output)
