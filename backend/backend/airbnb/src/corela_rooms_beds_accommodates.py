import os

from pyspark.sql import SparkSession, types
import pyspark.sql.dataframe
import pyspark.sql.functions as F
import sys
import json
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+


# add more functions as necessary
def main(input, output):

    listings_dir = input + '/' + 'listings'
    ## correlation between rooms beds and accommodates
    listings = spark.read.parquet(listings_dir)
    x = listings.select('bedrooms', 'beds', 'accommodates')
    features = 'corr_features'

    assembler = VectorAssembler(inputCols=x.columns,
                                outputCol=features, handleInvalid='keep')
    x_transformed = assembler.transform(x).select(features)

    # correlation will be in Matrix
    correlation = Correlation.corr(x_transformed, features, "pearson").collect()[0][0]

    # To convert Matrix into DataFrame
    rows = correlation.toArray().tolist()
    cols = x.columns
    correlation_df = spark.createDataFrame(rows, x.columns)
    diretory = '/corela_rooms_beds_accommodates'
    output_path = output + diretory

    correlation_df.coalesce(1).write.csv(output_path, mode='overwrite', header='true')

if __name__ == '__main__':
    # raw data path
    # data
    input = sys.argv[1]
    # processed data path
    output = '../analysis_results'
    spark = SparkSession.builder.appName('correlation between rooms beds and accommodate').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(input, output)
