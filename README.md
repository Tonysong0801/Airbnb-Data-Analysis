# This README.md is an instruction about data loading, processing and analysis. If you want to start the server to run and display our entire project, please follow the instructions in RUNNING.txt
### 0 Getting Started
#### Installation:
* pyspark
```
pip install pyspark
```
* numpy
```
pip install numpy
```
* keras
```
pip install keras
```
* sklearn
```
pip install scikit-learn
```
* tensorflow
```
pip install tensorflow
```
* pandas
```
pip install pandas
```
* wordcloud
```
pip install wordcloud
```
* matplotlib
```
pip install matplotlib
```
* xgboost
```
pip install xgboost
```
* nltk
```
pip install nltk
```
* langdetect
```
pip install langdetect
```
### 1 Data Cleaning
#### 1.1. Data Link
Since the rawData and the data after cleaning are large, so we upload the data in Google Drive. Please download the datasets and put them to the right path.
The datasets have 3 parts. One is rawData which we combine all the data download from 'Inside Airbnb'.
One is version1(V1AfterDataProcess) data in Parquet format after data processed. The last one is version2(V2AfterDataProcess) data in csv format.
The reason why we have 2 versions of datasets is we want to compare the performance of these file formats.

Link: https://drive.google.com/drive/folders/1fS4aLfhWBYxeGBRwATOQqCe1igD7329_?usp=share_link
#### 1.2. File Path  
In order to visualize the results conveniently, we put all the output files in cmput732proj/backend/backend/airbnb/analysis_results directory.

1.2.1 raw data path: rawData / COUNTRY_NAME / CITY_NAME / listings.csv.gz reviews.csc.gz

1.2.2 cleaned data
 path: data / listings   reviews
#### 1.3 Format
1.3.1 format: parquet & csv

#### 1.4 Run Data Process
Download the V2AfterDataProcess from the link above and unzip listings_detailed.zip and Vancouver_all.zip to your local.  
Download the rawData data from the link above and unzip it to cmput732proj/dataProcess, then run 'dataProcess.py' to obtain the cleaned data(cmput732proj/dataProcess/data)

### 2 ETL(Extract, Transform, Load)
We put all our ETL python code in cmput732proj/backend/backend/airbnb/src directory. To run our ETL python code, you need to configure the environment correctly by installing the packages mentioned above in '0 Getting Started' and put the data set in right places as mentioned in 1.4  
Here is what these codes do and how to run them in a Linux environment(in cmput732proj/backend/backend/airbnb/src):  
#### 1)priceWithTime.py: Obtain the mean and median house prices in the Vancouver area by week and month
command: ${SPARK_HOME}/bin/spark-submit priceWithTime.py YourLocalComputer/Vancouver_all/van_calendar.csv ../analysis_results/priceWithMonth ../analysis_results/priceWithWeek

#### 2)priceWithSpace.py: Obtain the average house price for each neighborhood in a specific Canadian region(there are 7 of them, which one it is depends on your command line argument, the following example uses listings5.csv) and its average latitude and longitude
command: ${SPARK_HOME}/bin/spark-submit priceWithSpace.py YourLocalComputer/listings_detailed/listings5.csv ../analysis_results/priceWithSpace

#### 3)priceWithType.py: Obtain the average, median, highest, and lowest prices for four types of homes in the Vancouver area
command: ${SPARK_HOME}/bin/spark-submit priceWithType.py YourLocalComputer/Vancouver_all/van_listings_detailed.csv ../analysis_results/priceWithType

#### 4)recommendation.py: A recommendation system that recommends several similar houses to the user based on the user's current house in Vancouver
command: ${SPARK_HOME}/bin/spark-submit recommendation.py YourLocalComputer/Vancouver_all/van_listings_detailed.csv

#### 5)avg_price_license.py:  Obtain the average price by having License or not (inputData is cmput732proj/dataProcess/data)
command: ${SPARK_HOME}/bin/spark-submit avg_price_license.py inputData ../analysis_results  

#### 6)corela_rooms_beds_accommodates.py: Obtain the correlation between bedrooms_num, beds_num and accommodates in Canada (inputData is cmput732proj/dataProcess/data)
command: ${SPARK_HOME}/bin/spark-submit corela_rooms_beds_accommodates.py inputData ../analysis_results  

#### 7)price_numberOfbedRoom.py:  Obtain the price distribution by BedRoom numbers in Canada (inputData is cmput732proj/dataProcess/data)
command: ${SPARK_HOME}/bin/spark-submit price_numberOfbedRoom.py inputData ../analysis_results  

#### 8)predict_price.py:  Generate the Toronto region price predition training model (inputData is cmput732proj/dataProcess/data)
command: ${SPARK_HOME}/bin/spark-submit predict_price.py inputData ../analysis_results

#### 9)review_analytics.py:  Analyze review scores and comments (inputData is cmput732proj/dataProcess/data)
command: ${SPARK_HOME}/bin/spark-submit review_analytics.py inputData ../analysis_results


    
