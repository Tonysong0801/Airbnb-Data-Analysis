import os

from pyspark.sql import SparkSession, types
import pyspark.sql.dataframe
import pyspark.sql.functions as F
import sys
import json
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+


# add more functions as necessary
def main(input, output):

    listings_dir = input + '/' + 'listings'
    ## price with the number of bedrooms
    ## 1 2 3 4 5 over_6
    listings = spark.read.parquet(listings_dir)
    bedRoom_num = listings.select('bedrooms', 'price').\
        withColumn('bedrooms_nums', F.when(F.col('bedrooms') > 5, '-1'). ## for frontend => change to over_5

                   otherwise(F.col('bedrooms').cast('string'))).select('bedrooms_nums', 'price')
    bedRoom_num.createOrReplaceTempView("bedRoom_num")
    bedRoom_num = spark.sql("SELECT bedrooms_nums, MIN(price) AS min, percentile_approx(price, 0.25) AS Q1,\
     percentile_approx(price, 0.5)AS median, percentile_approx(price, 0.75) AS Q3, MAX(price) AS max \
                FROM bedRoom_num \
                GROUP BY bedrooms_nums").orderBy('bedrooms_nums')

    diretory = '/price_numberOfBedRoom'
    output_path = output + diretory
    bedRoom_num.write.csv(output_path, mode='overwrite', header='true')

if __name__ == '__main__':
    # raw data path
    ## data
    input = sys.argv[1]
    # processed data path
    output = '../analysis_results'
    spark = SparkSession.builder.appName('price number of bed room').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(input, output)
