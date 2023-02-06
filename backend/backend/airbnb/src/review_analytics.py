import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import os

from pyspark.sql import SparkSession, types
import pyspark.sql.functions as F
from pyspark import SparkConf, SparkContext

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

import xgboost as xgb
import nltk
from langdetect import detect
from nltk.sentiment import SentimentIntensityAnalyzer
from wordcloud import WordCloud
import operator


def summary_statistics(l):

    #avg review scores by city
    by_city = l.groupby('city').agg(F.round(F.avg('review_scores_rating'),2), F.round(F.avg('review_scores_accuracy'),2),\
        F.round(F.avg('review_scores_cleanliness'),2), F.round(F.avg('review_scores_checkin'),2),\
            F.round(F.avg('review_scores_communication'),2), F.round(F.avg('review_scores_location'),2),\
                F.round(F.avg('review_scores_value'),2)).toDF('city','avg_overall', 'avg_accuracy', 'avg_cleanliness', 'avg_checkin',\
                    'avg_communication', 'avg_location', 'avg_value').sort('avg_overall', ascending = False)
    by_city.toPandas().to_csv(outputs + '/' + 'avg review scores by city', index=False)
    
    #avg review scores by room type
    by_room_type = l.groupby('room_type').agg(F.round(F.avg('review_scores_rating'),2), F.round(F.avg('review_scores_accuracy'),2),\
        F.round(F.avg('review_scores_cleanliness'),2), F.round(F.avg('review_scores_checkin'),2),\
            F.round(F.avg('review_scores_communication'),2), F.round(F.avg('review_scores_location'),2),\
                F.round(F.avg('review_scores_value'),2)).toDF('room_type','avg_overall', 'avg_accuracy', 'avg_cleanliness', 'avg_checkin',
                'avg_communication', 'avg_location', 'avg_value').sort('avg_overall', ascending = False)
    by_room_type.toPandas().to_csv(outputs + '/' + 'avg review scores by room type', index=False)

    #investigate the distribution of prices then aggregate review scores by price bucket
    price_percentile = l.select(F.percentile_approx('price', [0.25, 0.5, 0.75]))
    price_percentile.show()
    l = l.withColumn('price_bucket', F.when(l['price'] > 200, '>200').when(l['price'] > 150, '150-200').\
        when(l['price'] > 100, '100-150').when(l['price'] > 50, '50-100').otherwise('<=50'))
    by_price_bucket = l.groupby('price_bucket').agg(F.round(F.avg('review_scores_rating'),2), F.round(F.avg('review_scores_accuracy'),2),\
        F.round(F.avg('review_scores_cleanliness'),2), F.round(F.avg('review_scores_checkin'),2),\
            F.round(F.avg('review_scores_communication'),2), F.round(F.avg('review_scores_location'),2),\
                F.round(F.avg('review_scores_value'),2)).toDF('price_bucket','avg_overall', 'avg_accuracy', 'avg_cleanliness', 'avg_checkin',
                'avg_communication', 'avg_location', 'avg_value').sort('avg_overall', ascending = False)
    by_price_bucket.toPandas().to_csv(outputs + '/' + 'avg review scores by price bucket', index=False)

    #aggregate review scores by price_per_person bucket
    l = l.withColumn('price_per_person', l['price']/l['accommodates'])
    price_per_person_percentile = l.select(F.percentile_approx('price_per_person', [0.25, 0.5, 0.75]))
    price_per_person_percentile.show()
    l = l.withColumn('price_per_person_bucket', F.when(l['price_per_person'] > 65, '>65').when(l['price'] > 45, '45-65').\
        when(l['price'] > 30, '30-45').otherwise('<=30'))
    by_per_person_price_bucket = l.groupby('price_per_person_bucket').agg(F.round(F.avg('review_scores_rating'),2), F.round(F.avg('review_scores_accuracy'),2),\
        F.round(F.avg('review_scores_cleanliness'),2), F.round(F.avg('review_scores_checkin'),2),\
            F.round(F.avg('review_scores_communication'),2), F.round(F.avg('review_scores_location'),2),\
                F.round(F.avg('review_scores_value'),2)).toDF('price_per_person_bucket','avg_overall', 'avg_accuracy', 'avg_cleanliness', 'avg_checkin',
                'avg_communication', 'avg_location', 'avg_value').sort('avg_overall', ascending = False)
    by_per_person_price_bucket.show()
    
    #avg review scores for superhost vs non-superhost
    by_host = l.filter(~l['host_is_superhost'].isNull()).groupby('host_is_superhost').agg(F.round(F.avg('review_scores_rating'),2), F.round(F.avg('review_scores_accuracy'),2),\
        F.round(F.avg('review_scores_cleanliness'),2), F.round(F.avg('review_scores_checkin'),2),\
            F.round(F.avg('review_scores_communication'),2), F.round(F.avg('review_scores_location'),2),\
                F.round(F.avg('review_scores_value'),2)).toDF('host_is_superhost','avg_overall', 'avg_accuracy', 'avg_cleanliness', 'avg_checkin',
                'avg_communication', 'avg_location', 'avg_value').sort('avg_overall', ascending = False)
    by_host.toPandas().to_csv(outputs + '/' + 'avg review scores for superhosts vs non-superhosts', index=False)
    return l


def feature_engineering(l):
    
    #how long has host used airbnb
    l = l.withColumn('host_time_on_airbnb', F.datediff(F.current_date(),l['host_since']))
    #calculate bathrooms per max occupants
    l = l.withColumn('bathroom_per_person', l['bathrooms']/l['accommodates'])
    #calculate beds per bedrooms
    l = l.withColumn('bed_per_bedroom', l['beds']/l['bedrooms'])
    # convert host acceptance rate to percentage
    l = l.withColumn('tmp', F.split(l['host_acceptance_rate'], '%').getItem(0))
    l = l.withColumn('host_acceptance_rate_pct', F.when(l['tmp'].isNotNull(),l['tmp']/100).otherwise(l['host_acceptance_rate']).cast('float'))

    XY = l.select('host_response_time', 'host_is_superhost', 'host_identity_verified', 
    'room_type', 'license', 'city', 
    'host_time_on_airbnb',  'host_acceptance_rate_pct', 'host_listings_count',  'price_per_person',
    'bathroom_per_person', 'bed_per_bedroom',  
    'review_scores_accuracy', 'review_scores_cleanliness',
    'review_scores_checkin', 'review_scores_communication', 'review_scores_location',
    'review_scores_value', 'reviews_per_month', 'review_scores_rating').toPandas()


    #mark categorical variables
    n_categorical_features = 6
    XY.iloc[:,0:n_categorical_features] = XY.iloc[:,0:n_categorical_features].astype("category")

    #remove NA values and randomly shuffle data
    XY = XY.replace('N/A', np.nan).replace('null', np.nan).replace("", np.nan)
    XY = XY.dropna()
    XY = XY.sample(frac=1)
    
    y = XY.iloc[:,-1]
    X = XY.iloc[:,:-1]
    return X, y


def regression(l):

    X, y = feature_engineering(l)

    #cross validation to get average R2 scores and feature importances
    Num_folds = 5
    fold_size = X.shape[0]//Num_folds
    
    r2_train = []
    r2_val = []
    feature_importance = []

    for f in range(Num_folds):
      
      X_val = X[f*fold_size: (f+1)*fold_size]
      y_val = y[f*fold_size: (f+1)*fold_size]

      val_mask = np.arange(f*fold_size, (f+1)*fold_size)
      train_idx = ~np.isin(np.arange(len(X)), val_mask)
      X_train = X[train_idx]
      y_train = y[train_idx]

      model = xgb.XGBRegressor(tree_method="hist", enable_categorical=True, max_depth=5, max_cat_to_onehot=10)
      model.fit(X_train, y_train)

      predictions = model.predict(X_val)
      print(pd.DataFrame({'target': y_val, 'prediction': predictions}))

      r2_train.append(model.score(X_train, y_train))
      r2_val.append(model.score(X_val, y_val))
      feature_importance.append(model.feature_importances_)


    print('Average training R2:', np.mean(r2_train))
    print('Average validation R2:', np.mean(r2_val))

    #generate feature importance summary
    feature_importance_mean = np.mean(feature_importance, axis=0)
    feature_importance_std = np.std(feature_importance, axis=0)
    sorted_idx = (-feature_importance_mean).argsort()
    feature_names = list(X.columns)

    feature_importance_summary = pd.DataFrame({'feature': np.array(feature_names)[sorted_idx],\
                   'feature_importance_mean': feature_importance_mean[sorted_idx],\
                   'feature_importance_std': feature_importance_std[sorted_idx]
                  })

    #sub-category review scores most significant in predicting overall review scores
    t1 = feature_importance_summary[feature_importance_summary['feature'].str.contains('review_scores')]
    t1.to_csv(outputs + '/' + 'sub-category review scores most significant in predicting overall review scores', index=False)
    #other factors that are most significant in predicting overall review score 
    t2 = feature_importance_summary[~feature_importance_summary['feature'].str.contains('review_scores')].head(5)
    t2.to_csv(outputs + '/' + 'other factors that are most significant in predicting overall review scores', index=False)
 


def detect_language(row):
    try:
        language = detect(row['comments'])
    except:
        language = "error"
    if language == "en":
        return row


def classify_sentiment(sia, row):
   
    scores = [
        sia.polarity_scores(sentence)["compound"]
        for sentence in nltk.sent_tokenize(row['comments'])
    ]
    return np.mean(scores)


def tokenizer(row):
    words = nltk.word_tokenize(row['comments'])
    for w in words:
        yield (w.lower(), 1)


def get_count(kv):
    return kv[1]


def reviews_text_analytics(r):

    #focus analysis on English language reviews for now
    r = r.map(detect_language).filter(lambda x: x is not None).cache()
    
    #classify review sentiment
    sia = SentimentIntensityAnalyzer()
    r_pos = r.filter(lambda x: classify_sentiment(sia, x)>=0)
    r_neg = r.filter(lambda x: classify_sentiment(sia, x)<0)

    nltk.download("stopwords")
    stopwords = nltk.corpus.stopwords.words("english")

    #rank most frequent words used in positive and negative reviews
    r_neg_words = r_neg.flatMap(tokenizer).filter(lambda kv: kv[0] not in stopwords and kv[0].isalpha()).\
        reduceByKey(operator.add).sortBy(get_count, ascending = False)

    r_pos_words = r_pos.flatMap(tokenizer).filter(lambda kv: kv[0] not in stopwords and kv[0].isalpha()).\
        reduceByKey(operator.add).sortBy(get_count, ascending = False)

    #filter out common words that appear in both lists
    top_pos_words = pd.DataFrame(r_pos_words.take(100), columns = ['word', 'freq'])
    top_neg_words = pd.DataFrame(r_neg_words.take(100), columns = ['word', 'freq'])
    uniq_pos_words = pd.merge(top_pos_words, top_neg_words, 'left', 'word')
    uniq_pos_words = uniq_pos_words.loc[pd.isna(uniq_pos_words['freq_y'])]
    uniq_neg_words = pd.merge(top_neg_words, top_pos_words, 'left', 'word')
    uniq_neg_words = uniq_neg_words.loc[pd.isna(uniq_neg_words['freq_y'])]


    #generate wordcloud
    pos_words_dict = dict(uniq_pos_words.iloc[:,:-1].to_records(index=False))
    neg_words_dict = dict(uniq_neg_words.iloc[:,:-1].to_records(index=False))
    wordcloud1 = WordCloud(width=2000, height=1000).generate_from_frequencies(pos_words_dict)
    plt.imshow(wordcloud1, interpolation='bilinear')
    plt.axis("off")
    plt.show()
    wordcloud2 = WordCloud(width=2000, height=1000).generate_from_frequencies(neg_words_dict)
    plt.imshow(wordcloud2, interpolation='bilinear')
    plt.axis("off")
    plt.show()


# add more functions as necessary
def main(inputs, outputs):

    listings = spark.read.parquet(inputs + '/' + 'listings')
    reviews = spark.read.parquet(inputs + '/' + 'reviews')
    
    #focus analysis on listings in Canada and filter out listings with no review scores
    listings = listings.filter(listings['country'] == 'Canada').filter(~listings['review_scores_rating'].isNull()).cache()

    if not os.path.exists(outputs):
        os.mkdir(outputs)

    #summary statistics of review scores
    listings = summary_statistics(listings)

    #find factors that predict overall review scores
    regression(listings)
    
    #analyze review comments, focus analysis on reviews written in 2022 for now
    reviews = reviews.filter(reviews['country'] == 'Canada')
    reviews = reviews.withColumn('review_year', F.year(reviews['date']))
    reviews = reviews.filter(reviews['review_year'] == 2022)
    reviews_text_analytics(reviews.rdd)


if __name__ == '__main__':
    inputs = sys.argv[1]
    outputs = sys.argv[2]
    spark = SparkSession.builder.appName('review analytics').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, outputs)
    conf = SparkConf().setAppName('review analytics')
    sc = SparkContext(conf=conf)

