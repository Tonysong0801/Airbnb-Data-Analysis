import os

from pyspark.sql import SparkSession, types
import pyspark.sql.functions as F
import sys
import json
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+


# add more functions as necessary
def main(input, output):

    listings_dir = input + '/' + 'listings'
    ## averge price with license or not
    # (omit Victoria, Winnipeg and NewBrunswick since class imbalance)
    listings = spark.read.parquet(listings_dir)
    license_price = listings.filter("city not in ('Victoria', 'Winnipeg', 'NewBrunswick')").select('license', 'price', 'city')
    license_price_avg = license_price.groupBy('license').avg().\
        select('license', F.round(F.col('avg(price)'), 2).alias('avg_price'))

    diretory = '/avg_price_license'
    output_path = output + diretory
    license_price_avg.write.csv(output_path, mode='overwrite', header='true')

if __name__ == '__main__':
    # raw data path
    ## data
    input = sys.argv[1]
    # processed data path
    output = '../analysis_results'
    spark = SparkSession.builder.appName('avg price license').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(input, output)
