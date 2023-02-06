import sys

from keras import estimator
from sklearn.utils import check_array
from sklearn.utils.extmath import safe_sparse_dot

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql.functions import approx_count_distinct,collect_list
from pyspark.sql.functions import collect_set,sum,avg,max,countDistinct,count
from pyspark.sql.functions import first, last, kurtosis, min, mean, skewness
from pyspark.sql.functions import stddev, stddev_samp, stddev_pop, sumDistinct,month
from pyspark.sql.functions import variance,var_samp,  var_pop, when,col
from pyspark.sql import SparkSession, functions, types
from pyspark.sql import functions as fn
from pyspark.sql.functions import substring
import math

import pandas as pd
import matplotlib.pyplot as plt
from wordcloud import WordCloud, STOPWORDS
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel, check_pairwise_arrays



def main(input):
    input_data = pd.read_csv(input, usecols=['id', 'name', 'description'], encoding='ISO-8859-1')
    input_data.head(10)

    input_data['name'] = input_data['name'].astype('str')
    input_data['description'] = input_data['description'].astype('str')

    corp = ' '.join(input_data['name'])
    desc_corp = ' '.join(input_data['description'])

    '''
    name_wordcloud = WordCloud(stopwords = STOPWORDS, background_color = 'white', height = 2000, width = 4000).generate(corp)
    plt.figure(figsize = (16,8))
    plt.imshow(name_wordcloud)
    plt.axis('off')
    plt.show()

    description_wordcloud = WordCloud(stopwords = STOPWORDS, background_color = 'white', height = 2000, width = 4000).generate(desc_corp)
    plt.figure(figsize = (16,8))
    plt.imshow(description_wordcloud)
    plt.axis('off')
    plt.show()
    '''

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
        desc = ' \nDescription: ' + input_data.loc[input_data['id'] == id]['content'].tolist()[0].split(' // ')[1][
                                    0:165] + '...'
        prediction = name + desc
        return prediction

    def recommend(item_id, num):
        print('Recommending ' + str(num) + ' products similar to ' + item(item_id))
        print('---')
        recs = results[item_id][:num]
        for rec in recs:
            print('\nRecommended: ' + item(rec[1]) + '\n(score:' + str(rec[0]) + ')')

    recommend(item_id=13490, num=5)

if __name__ == '__main__':

    '''spark = SparkSession.builder.appName('priceWithMonth').getOrCreate()
    assert spark.version >= '3.0'
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext'''

    '''
        In our project, we set:
        input= "YourLocalComputer\\Vancouver_all\\van_listings_detailed.csv"
    '''

    input = sys.argv[1]
    main(input)

