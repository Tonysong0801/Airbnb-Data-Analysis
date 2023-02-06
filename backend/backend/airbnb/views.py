from rest_framework import status
from rest_framework.decorators import api_view
from rest_framework.response import Response
from .models import *
from .serializers import *

import json
from pyspark.ml import PipelineModel

from .utils import *

# api views of kevin data
@api_view(['GET'])
def avg_price_license_list(request):
    """
    Data processed by Kevin
    Average Price by having License or not
    Return a json format of avg_price_license
    """
    if request.method == 'GET':
        avg_price_license = Avg_Price_License.objects.all()
        if avg_price_license.exists():
            serializer = Avg_Price_LicenseSerializer(avg_price_license, many=True)
        # init the data from reading the csv file
        else:
            read_avg_price_license()
            serializer = Avg_Price_LicenseSerializer(avg_price_license, many=True)
        # print(serializer.data)
        return Response(serializer.data)

    # elif request.method == 'POST':
    #     serializer = SnippetSerializer(data=request.data)
    #     if serializer.is_valid():
    #         serializer.save()
    #         return Response(serializer.data, status=status.HTTP_201_CREATED)
    #     return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


@api_view(['GET'])
def corela_rooms_beds_accommodates_list(request):
    """
    Data processed by Kevin
    Correlation between bedrooms_num, beds_num and accommodates
    Return a json format of corela_rooms_beds_accommodates
    """
    if request.method == 'GET':
        corela_rooms_beds_accommodates = Corela_Rooms_Beds_Accommodates.objects.all()
        if corela_rooms_beds_accommodates.exists():
            serializer = Corela_Rooms_Beds_AccommodatesSerializer(corela_rooms_beds_accommodates, many=True)
        # init the data from reading the csv file
        else:
            read_corela_rooms_beds_accommodates()
            serializer = Corela_Rooms_Beds_AccommodatesSerializer(corela_rooms_beds_accommodates, many=True)
        # print(serializer.data)
        return Response(serializer.data)

@api_view(['GET'])
def price_numberOfBedRoom_list(request):
    """
    Data processed by Kevin
    Price Distribution by BedRooms_number
    Return a json format of price_numberOfBedRoom
    """
    if request.method == 'GET':
        price_numberOfBedRoom = Price_NumberOfBedRoom.objects.all()
        if price_numberOfBedRoom.exists():
            serializer = Price_NumberOfBedRoomSerializer(price_numberOfBedRoom, many=True)
        # init the data from reading the csv file
        else:
            read_price_numberOfBedRoom()
            serializer = Price_NumberOfBedRoomSerializer(price_numberOfBedRoom, many=True)
        # print(serializer.data)
        return Response(serializer.data)




# api views of tony data
@api_view(['GET'])
def priceWithMonth_list(request):
    """
    Data processed by Tony
    Changes in the mean and median house prices in the Vancouver area by month
    Return a json format of priceWithMonth
    """
    if request.method == 'GET':
        price_with_month = priceWithMonth.objects.all()
        if price_with_month.exists():
            serializer = PriceWithMonthSerializer(price_with_month, many=True)
        # init the data from reading the csv file
        else:
            read_priceWithMonth()
            serializer = PriceWithMonthSerializer(price_with_month, many=True)
        # print(serializer.data)
        return Response(serializer.data)

@api_view(['GET'])
def priceWithSpace_list(request):
    """
    Data processed by Tony
    The average house price for each neighborhood in Vancouver and its average latitude and longitude
    Return a json format of price_with_space
    """
    if request.method == 'GET':
        price_with_space = priceWithSpace.objects.all()
        if price_with_space.exists():
            serializer = PriceWithSpaceSerializer(price_with_space, many=True)
        # init the data from reading the csv file
        else:
            read_priceWithSpace()
            serializer = PriceWithSpaceSerializer(price_with_space, many=True)
        # print(serializer.data)
        return Response(serializer.data)

@api_view(['GET'])
def priceWithType_list(request):
    """
    Data processed by Tony
    Average, median, highest, and lowest prices for four types of homes in the Vancouver area
    Return a json format of price_numberOfBedRoom
    """
    if request.method == 'GET':
        price_with_type = priceWithType.objects.all()
        if price_with_type.exists():
            serializer = PriceWithTypeSerializer(price_with_type, many=True)
        # init the data from reading the csv file
        else:
            read_priceWithType()
            serializer = PriceWithTypeSerializer(price_with_type, many=True)
        # print(serializer.data)
        return Response(serializer.data)

@api_view(['GET'])
def priceWithWeek_list(request):
    """
    Data processed by Tony
    Changes in the mean and median house prices in the Vancouver area by week
    Return a json format of priceWithWeek
    """
    if request.method == 'GET':
        price_with_week = priceWithWeek.objects.all()
        if price_with_week.exists():
            serializer = PriceWithWeekSerializer(price_with_week, many=True)
        # init the data from reading the csv file
        else:
            read_priceWithWeek()
            serializer = PriceWithWeekSerializer(price_with_week, many=True)
        # print(serializer.data)
        return Response(serializer.data)



# api views of calla data

@api_view(['GET'])
def avg_review_by_city_list(request):
    """
    Data processed by Calla
    avg review scores by city
    Return a json format of avg_review_by_city
    """
    if request.method == 'GET':
        avg_review_by_city = Avg_Review_by_City.objects.all()
        if avg_review_by_city.exists():
            serializer = Avg_Review_by_CitySerializer(avg_review_by_city, many=True)
        # init the data from reading the csv file
        else:
            read_avg_review_by_city()
            serializer = Avg_Review_by_CitySerializer(avg_review_by_city, many=True)
        # print(serializer.data)
        return Response(serializer.data)

@api_view(['GET'])
def avg_review_by_room_type_list(request):
    """
    Data processed by Calla
    avg review scores by room type
    Return a json format of avg_review_by_room_type
    """
    if request.method == 'GET':
        avg_review_by_room_type = Avg_Review_by_RoomType.objects.all()
        if avg_review_by_room_type.exists():
            serializer = Avg_Review_by_RoomTypeSerializer(avg_review_by_room_type, many=True)
        # init the data from reading the csv file
        else:
            read_avg_review_by_room_type()
            serializer = Avg_Review_by_RoomTypeSerializer(avg_review_by_room_type, many=True)
        # print(serializer.data)
        return Response(serializer.data)

@api_view(['GET'])
def avg_review_by_price_bucket_list(request):
    """
    Data processed by Calla
    avg review scores by city
    Return a json format of avg_review_by_price_bucket
    """
    if request.method == 'GET':
        avg_review_by_price_bucket = Avg_Review_by_PriceBucket.objects.all()
        if avg_review_by_price_bucket.exists():
            serializer = Avg_Review_by_PriceBucketSerializer(avg_review_by_price_bucket, many=True)
        # init the data from reading the csv file
        else:
            read_avg_review_by_price_bucket()
            serializer = Avg_Review_by_PriceBucketSerializer(avg_review_by_price_bucket, many=True)
        # print(serializer.data)
        return Response(serializer.data)

@api_view(['GET'])
def avg_review_by_superhosts_list(request):
    """
    Data processed by Calla
    avg review scores by city
    Return a json format of avg_review_by_superhosts
    """
    if request.method == 'GET':
        avg_review_by_superhosts = Avg_Review_by_Superhosts.objects.all()
        if avg_review_by_superhosts.exists():
            serializer = Avg_Review_by_SuperhostsSerializer(avg_review_by_superhosts, many=True)
        # init the data from reading the csv file
        else:
            read_avg_review_by_superhosts()
            serializer = Avg_Review_by_SuperhostsSerializer(avg_review_by_superhosts, many=True)
        # print(serializer.data)
        return Response(serializer.data)


@api_view(['GET'])
def factors_predict_review_list(request):
    """
    Data processed by Calla
    Other factors that mostly stongly predict overall review score
    Return a json format of factors_predict_review
    """
    if request.method == 'GET':
        factors_predict_review = Factors_Predict_Review.objects.all()
        if factors_predict_review.exists():
            serializer = Factors_Predict_ReviewSerializer(factors_predict_review, many=True)
        # init the data from reading the csv file
        else:
            read_factors_predict_review()
            serializer = Factors_Predict_ReviewSerializer(factors_predict_review, many=True)
        # print(serializer.data)
        return Response(serializer.data)

@api_view(['GET'])
def sub_category_predict_review_list(request):
    """
    Data processed by Calla
    sub-category review scores that mostly strongly predict overall review score
    Return a json format of sub_category_predict_review
    """
    if request.method == 'GET':
        sub_category_predict_review = SubCategory_Predict_Review.objects.all()
        if sub_category_predict_review.exists():
            serializer = SubCategory_Predict_ReviewSerializer(sub_category_predict_review, many=True)
        # init the data from reading the csv file
        else:
            read_sub_category_predict_review()
            serializer = SubCategory_Predict_ReviewSerializer(sub_category_predict_review, many=True)
        # print(serializer.data)
        return Response(serializer.data)


@api_view(['POST'])
def predict_price(request):
    """
    Advanced feature by Kevin
    Use trained model to predict the price with the following input
    'accommodates','bedrooms','beds','license','bathrooms'
    the following two arguments are ignored due to some technical issues
    'room_type','neighbourhood_cleansed'
    """
    if request.method == 'POST':
        predict_data = request.data
        print('req data---------------------------',request.data)
        try:
            result = price_predict(predict_data)
            return_data = {'data': result}
            return_data['success']=1
        except Exception as e:
            return_data = {'success': 0}
            return_data = {'error': str(e)}
        return Response(return_data)

@api_view(['POST'])
def recommendation(request):
    """
    Advanced feature by Tony
    """
    if request.method == 'POST':
        predict_data = request.data
        try:
            result = house_recommend(int(predict_data['id']), int(predict_data['nums']))
            print('------------------------------',result)
            return_data = {'data': result}
            return_data['success']=1
        except Exception as e:
            return_data = {'success': 0}
            return_data = {'error': str(e)}
        return Response(return_data)