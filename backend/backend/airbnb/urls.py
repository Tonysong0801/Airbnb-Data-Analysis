from django.urls import path
from airbnb import views

urlpatterns = [
    # kevin
    path('avg_price_license/', views.avg_price_license_list),
    path('corela_rooms_beds_accommodates/', views.corela_rooms_beds_accommodates_list),
    path('price_numberOfBedRoom/', views.price_numberOfBedRoom_list),

    # tony
    path('priceWithMonth/', views.priceWithMonth_list),
    path('priceWithSpace/', views.priceWithSpace_list),
    path('priceWithType/', views.priceWithType_list),
    path('priceWithWeek/', views.priceWithWeek_list),

    # calla
    path('avg_review_by_city/', views.avg_review_by_city_list),
    path('avg_review_by_room_type/', views.avg_review_by_room_type_list),
    path('avg_review_by_price_bucket/', views.avg_review_by_price_bucket_list),
    path('avg_review_by_superhosts/', views.avg_review_by_superhosts_list),

    path('factors_predict_review/', views.factors_predict_review_list),
    path('sub_category_predict_review/', views.sub_category_predict_review_list),

    # advanced features
    path('predict_price/', views.predict_price),
    path('recommendation/', views.recommendation),
]