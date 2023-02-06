import csv
import json
import os
from django.conf import settings

from pyspark.sql import types,SparkSession
from pyspark.ml import PipelineModel
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline
import pyspark.sql.functions as F

from keras import estimator
from sklearn.utils import check_array
from sklearn.utils.extmath import safe_sparse_dot
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel, check_pairwise_arrays

from .models import *
from .serializers import *

'''
data from kevin
read the csv file and init the data to database
'''
def read_avg_price_license():
    file_name = "airbnb/analysis_results/avg_price_license/avg_price_license.csv"
    with open(os.path.join(settings.BASE_DIR, file_name), newline='') as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:
            data = {'license': row['license'], 'avg_price': float(row['avg_price'])}
            serializer = Avg_Price_LicenseSerializer(data=data)
            if serializer.is_valid():
                serializer.save()
            else:
                print("!!!-------Avg_Price_LicenseSerializer failed!")

'''
data from kevin
read the csv file and init the data to database
'''
def read_corela_rooms_beds_accommodates():
    file_name = "airbnb/analysis_results/corela_rooms_beds_accommodates/corela_rooms_beds_accommodates.csv"
    with open(os.path.join(settings.BASE_DIR, file_name), newline='') as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:
            data = {'bedrooms': float(row['bedrooms']), 'beds': float(row['beds']), 'accommodates': float(row['accommodates'])}
            serializer = Corela_Rooms_Beds_AccommodatesSerializer(data=data)
            if serializer.is_valid():
                serializer.save()
            else:
                print("!!!-------Corela_Rooms_Beds_AccommodatesSerializer failed!")    


'''
data from kevin
read the csv file and init the data to database
'''
def read_price_numberOfBedRoom():
    file_name = "airbnb/analysis_results/price_numberOfBedRoom/price_numberOfBedRoom.csv"
    with open(os.path.join(settings.BASE_DIR, file_name), newline='') as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:
            data = {'bedrooms_nums': int(row['bedrooms_nums']), 'min': float(row['min']), 'q1': float(row['Q1']),
            'median': float(row['median']), 'q3': float(row['Q3']), 'max': float(row['max'])}
            serializer = Price_NumberOfBedRoomSerializer(data=data)
            if serializer.is_valid():
                serializer.save()
            else:
                print("!!!-------read_price_numberOfBedRoom failed!")  



'''
data from tony
read the csv file and init the data to database
'''
def read_priceWithMonth():
    file_name = "airbnb/analysis_results/priceWithMonth/priceWithMonth.csv"
    with open(os.path.join(settings.BASE_DIR, file_name), newline='') as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:
            data = {'month': row['Month'], 'average_price': float(row['average_price($)']), 'median_price': float(row['median_price($)'])}
            serializer = PriceWithMonthSerializer(data=data)
            if serializer.is_valid():
                serializer.save()
            else:
                print("!!!-------PriceWithMonthSerializer failed!")  

'''
data from tony
read the csv file and init the data to database
'''
def read_priceWithSpace():
    file_name = "airbnb/analysis_results/priceWithSpace/priceWithSpace.csv"
    with open(os.path.join(settings.BASE_DIR, file_name), newline='') as csvfile:
        reader = csv.DictReader(csvfile,delimiter='\t')

        for row in reader:
            data = {'neighbourhood': row['neighbourhood'], 'average_price': float(row['avg_price($)']), 
            'avg_latitute': float(row['avg_latitute']), 'avg_longitude': float(row['avg_longitude'])}
            serializer = PriceWithSpaceSerializer(data=data)
            if serializer.is_valid():
                serializer.save()
            else:
                print("!!!-------PriceWithSpaceSerializer failed!")  

'''
data from tony
read the csv file and init the data to database
'''
def read_priceWithType():
    file_name = "airbnb/analysis_results/priceWithType/priceWithType.csv"
    with open(os.path.join(settings.BASE_DIR, file_name), newline='') as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:
            data = {'room_type': row['room_type'], 'average_price': float(row['average_price($)']), 
            'median_price': float(row['median_price($)']), 'max_price': float(row['max_price($)']),
            'min_price': float(row['min_price($)'])}
            serializer = PriceWithTypeSerializer(data=data)
            if serializer.is_valid():
                serializer.save()
            else:
                print("!!!-------PriceWithTypeSerializer failed!")  

'''
data from tony
read the csv file and init the data to database
'''
def read_priceWithWeek():
    file_name = "airbnb/analysis_results/priceWithWeek/priceWithWeek.csv"
    with open(os.path.join(settings.BASE_DIR, file_name), newline='') as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:
            data = {'week': row['week'], 'average_price': float(row['average_price($)']),
            'median_price': float(row['median_price($)'])}
            serializer = PriceWithWeekSerializer(data=data)
            if serializer.is_valid():
                serializer.save()
            else:
                print("!!!-------PriceWithWeekSerializer failed!")  


'''
data from calla
read the csv file and init the data to database
'''
def read_avg_review_by_city():
    file_name = "airbnb/analysis_results/avg_review_by_city/avg_review_by_city.csv"
    with open(os.path.join(settings.BASE_DIR, file_name), newline='') as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:
            data = {'city': row['city'], 'avg_overall': float(row['avg_overall']), 'avg_accuracy': float(row['avg_accuracy']),
            'avg_cleanliness': float(row['avg_cleanliness']), 'avg_checkin': float(row['avg_checkin']), 
            'avg_communication': float(row['avg_communication']),
            'avg_location': float(row['avg_location']), 'avg_value': float(row['avg_value'])}
            serializer = Avg_Review_by_CitySerializer(data=data)
            if serializer.is_valid():
                serializer.save()
            else:
                print("!!!-------Avg_Review_by_CitySerializer failed!")  

'''
data from calla
read the csv file and init the data to database
'''
def read_avg_review_by_room_type():
    file_name = "airbnb/analysis_results/avg_review_by_room_type/avg_review_by_room_type.csv"
    with open(os.path.join(settings.BASE_DIR, file_name), newline='') as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:
            data = {'room_type': row['room_type'], 'avg_overall': float(row['avg_overall']), 'avg_accuracy': float(row['avg_accuracy']),
            'avg_cleanliness': float(row['avg_cleanliness']), 'avg_checkin': float(row['avg_checkin']), 
            'avg_communication': float(row['avg_communication']),
            'avg_location': float(row['avg_location']), 'avg_value': float(row['avg_value'])}
            serializer = Avg_Review_by_RoomTypeSerializer(data=data)
            if serializer.is_valid():
                serializer.save()
            else:
                print("!!!-------Avg_Review_by_RoomTypeSerializer failed!")  

'''
data from calla
read the csv file and init the data to database
'''
def read_avg_review_by_price_bucket():
    file_name = "airbnb/analysis_results/avg_review_by_price_bucket/avg_review_by_price_bucket.csv"
    with open(os.path.join(settings.BASE_DIR, file_name), newline='') as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:
            data = {'price_bucket': row['price_bucket'], 'avg_overall': float(row['avg_overall']), 'avg_accuracy': float(row['avg_accuracy']),
            'avg_cleanliness': float(row['avg_cleanliness']), 'avg_checkin': float(row['avg_checkin']), 
            'avg_communication': float(row['avg_communication']),
            'avg_location': float(row['avg_location']), 'avg_value': float(row['avg_value'])}
            serializer = Avg_Review_by_PriceBucketSerializer(data=data)
            if serializer.is_valid():
                serializer.save()
            else:
                print("!!!-------Avg_Review_by_PriceBucketSerializer failed!")  

'''
data from calla
read the csv file and init the data to database
'''
def read_avg_review_by_superhosts():
    file_name = "airbnb/analysis_results/avg_review_for_superhosts/avg_review_for_superhosts.csv"
    with open(os.path.join(settings.BASE_DIR, file_name), newline='') as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:
            data = {'host_is_superhost': row['host_is_superhost'], 'avg_overall': float(row['avg_overall']), 'avg_accuracy': float(row['avg_accuracy']),
            'avg_cleanliness': float(row['avg_cleanliness']), 'avg_checkin': float(row['avg_checkin']), 
            'avg_communication': float(row['avg_communication']),
            'avg_location': float(row['avg_location']), 'avg_value': float(row['avg_value'])}
            serializer = Avg_Review_by_SuperhostsSerializer(data=data)
            if serializer.is_valid():
                serializer.save()
            else:
                print("!!!-------Avg_Review_by_SuperhostsSerializer failed!")  


'''
data from calla
read the csv file and init the data to database
'''
def read_factors_predict_review():
    file_name = "airbnb/analysis_results/factors_predict_review/factors_predict_review.csv"
    with open(os.path.join(settings.BASE_DIR, file_name), newline='') as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:
            data = {'feature': row['feature'], 'feature_importance_mean': float(row['feature_importance_mean']), 
            'feature_importance_std': float(row['feature_importance_std']),}
            serializer = Factors_Predict_ReviewSerializer(data=data)
            if serializer.is_valid():
                serializer.save()
            else:
                print("!!!-------Factors_Predict_ReviewSerializer failed!")  

'''
data from calla
read the csv file and init the data to database
'''
def read_sub_category_predict_review():
    file_name = "airbnb/analysis_results/sub_category_predict_review/sub_category_predict_review.csv"
    with open(os.path.join(settings.BASE_DIR, file_name), newline='') as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:
            data = {'feature': row['feature'], 'feature_importance_mean': float(row['feature_importance_mean']), 
            'feature_importance_std': float(row['feature_importance_std']),}
            serializer = SubCategory_Predict_ReviewSerializer(data=data)
            if serializer.is_valid():
                serializer.save()
            else:
                print("!!!-------SubCategory_Predict_ReviewSerializer failed!")  


'''
Advanced feature by Kevin
Predict price
'''
def price_predict(data):
    spark = SparkSession.builder.appName('price train').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    sc = spark.sparkContext
    sc.setLogLevel('WARN')
    model_path = "airbnb/analysis_results/predict_price"
    model = PipelineModel.load(model_path)
    # data format: 'accommodates','bedrooms','beds','license','bathrooms','room_type','neighbourhood_cleansed'
    predict_data = [(float(data['accommodates']),float(data['bedrooms']),float(data['beds']),float(data['license']),float(data['bathrooms']),'','')]
    schema = types.StructType([
    types.StructField('accommodates', types.FloatType()),
    types.StructField('bedrooms', types.FloatType()),
    types.StructField('beds', types.FloatType()),
    types.StructField('license', types.FloatType()),
    types.StructField('bathrooms', types.FloatType()),
    types.StructField('room_type', types.StringType()),
    types.StructField('neighbourhood_cleansed', types.StringType()),
    # types.StructField('room_type_Scaled', types.FloatType()),
    # types.StructField('neighbourhood_cleansed_Scaled', types.FloatType()),
    ])
    predict = spark.createDataFrame(predict_data,schema)
    df = predict.select('accommodates', 'bedrooms', 'beds', F.col('license').cast('double').alias('license'),
                            'bathrooms', 'room_type', 'neighbourhood_cleansed')

    cate_features = [ 'room_type', 'neighbourhood_cleansed']

    ## categorical
    for f in cate_features:
        indexer = StringIndexer(inputCol=f, outputCol=f + "_Scaled",
                                            handleInvalid="error",
                                            stringOrderType="frequencyDesc")
        pipeline = Pipeline(stages=[
            indexer
        ])
        df = pipeline.fit(df).transform(df)

    df = df.select('accommodates',
                    'bedrooms',
                    'beds',
                    'license',
                    'bathrooms',
                    # 'city_Scaled',
                        'room_type_Scaled',
                        'neighbourhood_cleansed_Scaled').\
                                withColumn('accommodates', df['accommodates'] ** 0.5).\
                                withColumn('bathrooms', df['bathrooms'] ** 0.5).\
                                withColumn('bedrooms', df['bedrooms'] ** 0.5).\
                                withColumn('beds', df['beds'] ** 0.5)

    predictions = model.transform(df)
    result = predictions.select("prediction").collect()[0]['prediction']
    return result**2

def house_recommend(item_id, num):
    input_file = 'airbnb/analysis_results/recommend_data/recommend_data.csv'
    input_data = pd.read_csv(input_file, usecols=['id', 'name', 'description'], encoding='ISO-8859-1')
    input_data.head(10)

    input_data['name'] = input_data['name'].astype('str')
    input_data['description'] = input_data['description'].astype('str')

    corp = ' '.join(input_data['name'])
    desc_corp = ' '.join(input_data['description'])


    input_data['content'] = input_data[['name', 'description']].astype(str).apply(lambda x: ' // '.join(x), axis=1)
    input_data['content'].fillna('Null', inplace=True)

    def fit_transform(self, raw_documents, y=None):

        self._check_params()
        X = super(TfidfVectorizer, self).fit_transform(raw_documents)
        self._tfidf.fit(X)

        return self._tfidf.transform(X, copy=False)

    def linear_kernel(X, Y=None, dense_output=True):

        X, Y = check_pairwise_arrays(X, Y)
        return safe_sparse_dot(X, Y.T, dense_output=dense_output)

    def check_pairwise_arrays(X, Y, precomputed=False, dtype=None, warn_on_dtype=None):

        if Y is X or Y is None:
            X = Y = check_array(X, accept_sparse='csr', dtype=dtype, estimator=estimator)
        else:
            X = check_array(X, accept_sparse='csr', dtype=dtype, estimator=estimator)
            Y = check_array(Y, accept_sparse='csr', dtype=dtype, estimator=estimator)
        if precomputed:
            if X.shape[1] != Y.shape[0]:
                raise ValueError("Precomputed metric requires shape "
                                 "(n_queries, n_indexed). Got (%d, %d) "
                                 "for %d indexed." %
                                 (X.shape[0], X.shape[1], Y.shape[0]))
        elif X.shape[1] != Y.shape[1]:
            raise ValueError("Incompatible dimension for X and Y matrices: "
                             "X.shape[1] == %d while Y.shape[1] == %d" % (
                                 X.shape[1], Y.shape[1]))

        return X, Y

    tf = TfidfVectorizer(analyzer='word', ngram_range=(1, 2), min_df=0, stop_words='english')
    tfidf_matrix = tf.fit_transform(input_data['content'])

    similarity = linear_kernel(tfidf_matrix, tfidf_matrix)

    results = {}
    for idx, row in input_data.iterrows():
        similar_indices = similarity[idx].argsort()[:-100:-1]
        similar_items = [(similarity[idx][i], input_data['id'][i]) for i in similar_indices]
        results[row['id']] = similar_items[1:]

    def item(id):
        name = input_data.loc[input_data['id'] == id]['content'].tolist()[0].split(' // ')[0]
        desc = ' <br>Description: ' + input_data.loc[input_data['id'] == id]['content'].tolist()[0].split(' // ')[1][
                                    0:165] + '...'
        prediction = name + desc
        return prediction

    # def recommend(item_id, num):
    #     print('Recommending ' + str(num) + ' products similar to ' + item(item_id))
    #     print('---')
    #     recs = results[item_id][:num]
    #     for rec in recs:
    #         print('\nRecommended: ' + item(rec[1]) + '\n(score:' + str(rec[0]) + ')')
    return_value = []
    recs = results[item_id][:num]
    for rec in recs:
        return_value.append({'recommend':item(rec[1]), 'score': str(rec[0])})
    return return_value