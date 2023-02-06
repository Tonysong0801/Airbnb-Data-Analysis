from django.db import models

# Create your models here.


# models for kevin
class Avg_Price_License(models.Model):
    license = models.CharField(max_length=100)
    avg_price = models.FloatField()


class Corela_Rooms_Beds_Accommodates(models.Model):
    bedrooms = models.FloatField()
    beds = models.FloatField()
    accommodates = models.FloatField()


class Price_NumberOfBedRoom(models.Model):
    bedrooms_nums = models.IntegerField()
    min = models.FloatField()
    q1 = models.FloatField()
    median = models.FloatField()
    q3 = models.FloatField()
    max = models.FloatField()




# models for Tony

class priceWithMonth(models.Model):
    month = models.CharField(max_length=100)
    average_price = models.FloatField()
    median_price = models.FloatField()

class priceWithSpace(models.Model):
    neighbourhood = models.CharField(max_length=100)
    average_price = models.FloatField()
    avg_latitute = models.FloatField()
    avg_longitude = models.FloatField()

class priceWithType(models.Model):
    room_type = models.CharField(max_length=100)
    average_price = models.FloatField()
    median_price = models.FloatField()
    max_price = models.FloatField()
    min_price = models.FloatField()

class priceWithWeek(models.Model):
    week = models.CharField(max_length=100)
    average_price = models.FloatField()
    median_price = models.FloatField()


# models for Cella
class Avg_Review_by_City(models.Model):
    city = models.CharField(max_length=100)
    avg_overall = models.FloatField()
    avg_accuracy = models.FloatField()
    avg_cleanliness = models.FloatField()
    avg_checkin = models.FloatField()
    avg_communication = models.FloatField()
    avg_location = models.FloatField()
    avg_value = models.FloatField()

class Avg_Review_by_RoomType(models.Model):
    room_type = models.CharField(max_length=100)
    avg_overall = models.FloatField()
    avg_accuracy = models.FloatField()
    avg_cleanliness = models.FloatField()
    avg_checkin = models.FloatField()
    avg_communication = models.FloatField()
    avg_location = models.FloatField()
    avg_value = models.FloatField()

class Avg_Review_by_PriceBucket(models.Model):
    price_bucket = models.CharField(max_length=100)
    avg_overall = models.FloatField()
    avg_accuracy = models.FloatField()
    avg_cleanliness = models.FloatField()
    avg_checkin = models.FloatField()
    avg_communication = models.FloatField()
    avg_location = models.FloatField()
    avg_value = models.FloatField()

class Avg_Review_by_Superhosts(models.Model):
    host_is_superhost = models.CharField(max_length=100)
    avg_overall = models.FloatField()
    avg_accuracy = models.FloatField()
    avg_cleanliness = models.FloatField()
    avg_checkin = models.FloatField()
    avg_communication = models.FloatField()
    avg_location = models.FloatField()
    avg_value = models.FloatField()

class Factors_Predict_Review(models.Model):
    feature = models.CharField(max_length=100)
    feature_importance_mean = models.FloatField()
    feature_importance_std = models.FloatField()

class SubCategory_Predict_Review(models.Model):
    feature = models.CharField(max_length=100)
    feature_importance_mean = models.FloatField()
    feature_importance_std = models.FloatField()

