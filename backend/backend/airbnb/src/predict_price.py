import os

from pyspark.sql import SparkSession, types
import pyspark.sql.dataframe
import pyspark.sql.functions as F
import sys
import json
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler, Normalizer, StringIndexer, VectorIndexer, MinMaxScaler, RobustScaler, StandardScaler
from pyspark.ml.regression import RandomForestRegressor, LinearRegression, GBTRegressor
from pyspark.ml import Pipeline
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator, TrainValidationSplit, TrainValidationSplit
from pyspark.ml.evaluation import RegressionEvaluator
import pyspark.sql.types as T

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+


# add more functions as necessary
def main(input, output):

    listings_dir = input + '/' + 'listings'
    listings = spark.read.parquet(listings_dir)
    listings = listings.where((listings.country == 'Canada') & (listings.city == 'Toronto'))
    df = listings.select('accommodates', 'bedrooms', 'beds', 'price', F.col('license').cast('double').alias('license'),
                           'bathrooms', 'city', 'room_type', 'neighbourhood_cleansed')
    df = df.filter(~df['price'].isNull())

    to_array = F.udf(lambda v: v.toArray().tolist(), T.ArrayType(T.FloatType()))

    ## DATA
    cate_features = ['city', 'room_type', 'neighbourhood_cleansed']
    num_features = ['accommodates', 'bedrooms', 'beds', 'bathrooms']
    ## numeric
    # for f in num_features:
    #     assembler = VectorAssembler(inputCols=[f], outputCol=f + "_Vect")
    #     scaler = RobustScaler(inputCol=f + "_Vect", outputCol=f + "_Scaled")
    #
    #     pipeline = Pipeline(stages=[assembler, scaler])
    #
    #     # Fitting pipeline on dataframe
    #     df = pipeline.fit(df).transform(df).withColumn(f + "_Scaled", to_array(f + "_Scaled")).drop(f + "_Vect")
    #     df = df.withColumn(f+'_Scaled', df[f + '_Scaled'][0])
    ## categorical
    for f in cate_features:
        indexer = StringIndexer(inputCol=f, outputCol=f + "_Scaled",
                                             handleInvalid="error",
                                             stringOrderType="frequencyDesc")
        #assembler = VectorAssembler(inputCols=[f + "_Index"], outputCol=f + "_Vect")

        # MinMaxScaler Transformation
        # scaler = MinMaxScaler(inputCol=f + "_Vect", outputCol=f + "_Scaled")

        # Pipeline of VectorAssembler and MinMaxScaler
        pipeline = Pipeline(stages=[
            indexer,
            #assembler,
            #scaler
        ])
        df = pipeline.fit(df).transform(df)
        # Fitting pipeline on dataframe
       # df = pipeline.fit(df).transform(df).withColumn(f + "_Scaled", to_array(f + "_Scaled")).drop(f + "_Vect").drop(f + "_Index")
        #df = df.withColumn(f+'_Scaled', df[f + '_Scaled'][0])

    df = df.select('accommodates',
                   'bedrooms',
                   'beds',
                   'license',
                   'bathrooms',
                   # 'city_Scaled',
                    'room_type_Scaled',
                    'neighbourhood_cleansed_Scaled',
                   'price').where(F.col('price') != 0).withColumn('price', F.log2(F.col('price'))).\
                            withColumn('accommodates', df['accommodates'] ** 0.5).\
                            withColumn('bathrooms', df['bathrooms'] ** 0.5).\
                            withColumn('bedrooms', df['bedrooms'] ** 0.5).\
                              withColumn('beds', df['beds'] ** 0.5)
    #df.show(truncate=False)

    train_data, test_data = df.randomSplit([0.85, 0.15], seed=234)
    train_data = train_data.cache()
    test_data = test_data.cache()

    # ## num & cate features & label
    features = [
        'accommodates',
        'bedrooms',
        'beds',
        'license',
        'bathrooms',
        #'city_Scaled',
        'room_type_Scaled',
        'neighbourhood_cleansed_Scaled'
        ]
    feature = 'feature'
    labelCol = 'price'

    vec_asb = VectorAssembler(inputCols=features,
                                outputCol=feature)
    # norm = Normalizer().setP(1).setInputCol(feature).setOutputCol(feature_norm)
    #
    #
    regressor = RandomForestRegressor(featuresCol=feature, labelCol=labelCol, seed=101,
                                      #numTrees=10, maxDepth=5,
                                      maxBins=200)


    pl = Pipeline(stages=[
        vec_asb,
        regressor])
    #
    evaluator_rmse = RegressionEvaluator(predictionCol="prediction", labelCol=labelCol, metricName="rmse")
    evaluator_r2 = RegressionEvaluator(predictionCol="prediction", labelCol=labelCol, metricName="r2")

    paramGrid = (ParamGridBuilder().
                 addGrid(regressor.numTrees, [30, 40, 50]).
                 addGrid(regressor.maxDepth, [10, 15, 20]).
                 build())
    crossval = CrossValidator(estimator=pl,
                              estimatorParamMaps=paramGrid,
                              evaluator=evaluator_r2,
                              numFolds=7)
    #
    model = crossval.fit(train_data)
    model = model.bestModel
    print(model.stages)

    # model = pl.fit(train_data)
    # test_predict = model.transform(test_data)
    # train_predict = model.transform(train_data)
    # score_1 = evaluator_rmse.evaluate(test_predict)
    # score_2 = evaluator_rmse.evaluate(train_predict)
    # print('===rmse=======')
    # print(score_1)
    # print(score_2)
    # print('===r2=======')
    # score_1 = evaluator_r2.evaluate(test_predict)
    # score_2 = evaluator_r2.evaluate(train_predict)
    # print(score_1)
    # print(score_2)
    model_path = 'predict_price'
    model.write().overwrite().save(model_path)

if __name__ == '__main__':
    # raw data path
    # data
    input = sys.argv[1]
    # processed data path
    output = '../analysis_results'
    spark = SparkSession.builder.appName('predict price').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(input, output)
