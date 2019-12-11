import json
import collections


with open("./graph.json") as f:
    truedata = json.load(f)

# # print(len(truedata["actual_types"]))
# listofvals  = ["person_name", "business_name", "phone_number", "address", "street_name",
# "city", "neighborhood", "lat_lon_cord", "zip_code", "borough", "school_name",
# "color", "car_make", "city_agency", "area_of_study", "subject_in_school",
# "school_level", "college_name", "website", "building_classification",
# "vehicle_type", "location_type", "park_playground", "other"]

# truect = collections.defaultdict(int)

# for item in truedata["actual_types"]:
#     truect[item["manual_labels"][0]["semantic_type"]] += 1

# # print(truect)

# calcct = collections.defaultdict(int)

# with open("./task2.json") as f:
#         data = json.load(f)

# # print(len(data["predicted_types"]))

# list_of_cols = collections.defaultdict(list)

# for jsonval in data["predicted_types"]:
#     predictedVal = jsonval["semantic_types"][0]["label"]
#     calcct[jsonval["semantic_types"][0]["label"]] += 1
#     list_of_cols[predictedVal].append(jsonval["column_name"])
#     # if jsonval["column_name"] == "website":
#     #     print("Hello")

# # print(calcct)
# # print(list_of_cols["website"])


# def precision(list_of_cols, calcct, label):
#     print(list_of_cols[label])
#     print(calcct[label])


# # precision(list_of_cols, calcct, "zip_code")

# def recall(list_of_cols, truect, label):
#     print(list_of_cols[label])
#     print(truect[label])

# for lb in listofvals:
#     precision(list_of_cols, truect, lb)



# #website - 1 , 9/9
# #person_name - 8/15 , 8/15
# #business_name - 2/7 , 2/4
# #phone_number - 1 , 3/6
# #zip_code - 0
# #address - 6/17, 6/11
# #street_name - 1 , 3/11
# #city - 1 , 1/4
# #lat_lon_cord - 2 , 1/4
# #neighborhood - 1 , 1
# #borough - 1 , 4/6
# #school_name - 6/7 , 6/7
# #color - 1, 1
# #car_make - 5/5 , 5/7
# #city_agency - 1 , 14/14
# #area_of_study - 1, 1
# #subject_in_school - 5/5, 5/5
# #school_level - 5/6 , 5/7
# #college_name - 0 , 0
# #building_classification - 1 , 1
# #vehicle_type - 3/7, 4/8
# #location_type - 1/2 , 2/6
# #park_playground - 1 , 1/3
# #other - 0 , 0

# 108*2