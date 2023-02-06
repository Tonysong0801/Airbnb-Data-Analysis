
import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql.functions import approx_count_distinct,collect_list
from pyspark.sql.functions import collect_set,sum,avg,max,countDistinct,count
from pyspark.sql.functions import first, last, kurtosis, min, mean, skewness
from pyspark.sql.functions import stddev, stddev_samp, stddev_pop, sumDistinct,month
from pyspark.sql.functions import variance,var_samp,  var_pop, when,col
from pyspark.sql import SparkSession, functions, types
from pyspark.sql import functions as fn
from pyspark.sql.functions import substring

# add more functions as necessary

def main(input, output1,output2):
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

    calendar_schema = types.StructType([
        types.StructField('listing_id', types.IntegerType()),
        types.StructField('date', types.DateType()),
        types.StructField('available', types.StringType()),
        types.StructField('price', types.StringType()),
        types.StructField('adjusted_price', types.StringType()),
        types.StructField('minimum_nights', types.IntegerType()),
        types.StructField('maximum_nights', types.IntegerType()),
    ])


    '''
    spark.read.option("multiline", "true") \
        .option("quote", '"') \
        .option("header", "true") \
        .option("escape", "\\") \
        .option("escape", '"').csv("listings.csv")
    '''


    calendar_input=spark.read.option("multiline", "true") \
        .option("quote", '"') \
        .option("header", "true") \
        .option("escape", "\\") \
        .option("escape", '"').csv(input,schema=calendar_schema)
    calendar_input.show(20)
    calendar_input.printSchema()

    bool_calendar=calendar_input.withColumn('available',when(calendar_input.available=='f',\
                                                             False).otherwise(True))\
                                                             .withColumn('available',col('available')\
                                                             .cast(types.BooleanType()))
    #transform the 'available' from String to Boolean
    bool_calendar.show()
    bool_calendar.printSchema()

    standard_calendar=bool_calendar.select(*['listing_id','date','available', \
                                             substring(bool_calendar.price, pos=2,len=7).alias('price'), \
                                             substring(bool_calendar.adjusted_price, pos=2,len=7).alias('adjusted_price'),\
                                             'minimum_nights','maximum_nights',])\
                                             .withColumn('price',col('price').cast(types.IntegerType()))\
                                             .withColumn('adjusted_price',col('adjusted_price').cast(types.IntegerType()))
    # transform the 'price'&'adjusted_price' from String to Integer
    standard_calendar.show()
    standard_calendar.printSchema()

    #print(standard_calendar.count())

    distinct_calendar=standard_calendar.dropDuplicates(subset=[c for c in standard_calendar.columns if True])
    #drop all the duplicated rows in standard_calendar
    print(distinct_calendar.count())

    nullPriceFiltered=distinct_calendar.dropna(subset='price').cache() #frequently used later, important
    #filter all the row whose price is null
    #print(nullPriceFiltered.count())

    week = nullPriceFiltered.withColumn("week", fn.date_format(col("date"), "EEEE")) \
        .withColumn("Month", month(col("date"))).cache()
    # week.show()

    abbre2 = week.withColumn("Price", (col('price') + col('adjusted_price') / 2)) \
        .select(*['week', 'Price']).withColumnRenamed("Price", "priceWithWeek")
    # abbre2.show()

    abbre3 = abbre2.groupBy("week").agg(fn.avg("priceWithWeek").alias("average_price($)")\
                                        ,fn.percentile_approx('priceWithWeek',0.5).alias("median_price($)"))
    priceWithWeek = abbre3.withColumn('week', when(abbre3.week == 'Monday', 1) \
                                      .when(abbre3.week == 'Tuesday', 2) \
                                      .when(abbre3.week == 'Wednesday', 3) \
                                      .when(abbre3.week == 'Thursday', 4) \
                                      .when(abbre3.week == 'Friday', 5) \
                                      .when(abbre3.week == 'Saturday', 6) \
                                      .when(abbre3.week == 'Sunday', 7) \
                                      .otherwise(abbre3.week))


    unequal=nullPriceFiltered.select(*["listing_id","price","adjusted_price"]).\
                                    where(nullPriceFiltered.price!=nullPriceFiltered.adjusted_price)
    #select rows that have unequal prices
    unequal.show()
    unequal=unequal.withColumn("difference",col("price")-col("adjusted_price"))
    unequal.show()
    diff=unequal.select(*['listing_id','difference'])\
    .withColumnRenamed('listing_id','id').withColumnRenamed('difference','dif')
    #obtain the difference between prices
    diff.show()
    joined1=nullPriceFiltered.join(diff,[diff.id==unequal.listing_id])
    joined1.show()

    inspect=joined1.select(['id','minimum_nights','maximum_nights','dif'])\
                    .groupBy('id').agg(fn.max('dif').alias('max'),fn.count('maximum_nights').alias('count'))
    #inspect.show()


    extractMonth=nullPriceFiltered.withColumn('Month',month('date'))
    #extractMonth.show()

    #extract month from date
    extractMonth.show()

    finalPrice=extractMonth.withColumn('price2',(col('price')+col('adjusted_price'))/2)
    finalPrice.show()

    abbre=finalPrice.select(*['Month','Price2','listing_id']).withColumnRenamed("Price2","price")\
    .withColumnRenamed("listing_id","id")
    abbre.show()

    #finalMonth=abbre.groupBy("Month").agg(fn.avg("price").alias("average_price($)")).drop('id')
    finalMonth = abbre.groupBy("Month").agg(fn.avg("price").alias("average_price($)"),\
                                            fn.percentile_approx('price',0.5).alias("median_price($)")\
                                            ).drop('id')
    finalMonth=finalMonth.orderBy(finalMonth.Month.asc())
    finalMonth.show()

    priceWithMonth = finalMonth.withColumn('Month', when(finalMonth.Month == 1, 'January') \
                                         .when(finalMonth.Month == 2, 'February') \
                                         .when(finalMonth.Month == 3, 'March') \
                                         .when(finalMonth.Month == 4, 'April') \
                                         .when(finalMonth.Month == 5, 'May') \
                                         .when(finalMonth.Month == 6, 'June') \
                                         .when(finalMonth.Month == 7, 'July') \
                                         .when(finalMonth.Month == 8, 'August') \
                                         .when(finalMonth.Month == 9, 'September') \
                                         .when(finalMonth.Month == 10, 'October') \
                                         .when(finalMonth.Month == 11, 'November') \
                                         .when(finalMonth.Month == 12, 'December') \
                                         .otherwise(finalMonth.Month))

    priceWithMonth.show()
    priceWithMonth.write.csv(output1, mode='overwrite',header=True)



    priceWithWeek=priceWithWeek.orderBy(priceWithWeek.week.asc())\
                               .withColumn('week', when(priceWithWeek.week == 1,'Monday')\
                                         .when(priceWithWeek.week == 2,'Tuesday') \
                                         .when(priceWithWeek.week ==3 ,'Wednesday') \
                                         .when(priceWithWeek.week == 4,'Thursday') \
                                         .when(priceWithWeek.week == 5,'Friday') \
                                         .when(priceWithWeek.week ==6 ,'Saturday') \
                                         .when(priceWithWeek.week == 7,'Sunday') \
                                         .otherwise(priceWithWeek.week))

    priceWithWeek.show()

    priceWithWeek.write.csv(output2, mode='overwrite',header=True)





if __name__ == '__main__':
    spark = SparkSession.builder.appName('priceWithMonth').getOrCreate()
    assert spark.version>='3.0'
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    '''
    
    In our project, we set:
    input= "YourLocalComputer\\Vancouver_all\\van_calendar.csv"
    output1= "../analysis_results/priceWithMonth"
    output2= "../analysis_results/priceWithWeek"
    
    '''
    input = sys.argv[1]
    output1 = sys.argv[2]
    output2 = sys.argv[3]
    main(input,output1,output2)





