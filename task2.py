from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.types import StringType
from pyspark.sql.types import DoubleType
from pyspark.sql.types import IntegerType
from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark import SparkContext

import json
import pandas as pd
#import matplotlib.pyplot as plt
#from fuzzywuzzy import fuzz
#from jellyfish import soundex, levenshtein_distance as ld
#from jellyfish import levenshtein_distance as ld
#from Levenshtein import distance as ld
import re
#print("THis is starting : ",soundex("Anjan"))
#print("This is also starting : ",ld("anjan","anjana"))
# from pyspark.ml.feature import HashingTF, IDF, RegexTokenizer, StringIndexer, NGram
# from pyspark.ml.classification import LogisticRegression
# from pyspark.ml import Pipeline
# from pyspark.mllib.evaluation import MulticlassMetrics

#spark = SparkSession.builder.master("local[*]").getOrCreate()
sc = SparkContext("local", pyFiles=["jlf.zip"])
spark = SparkSession \
    .builder \
    .appName("Big Data Project") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
#sc = SparkContext()
#sc.addPyFile("jellyfish")
from jellyfish import levenshtein_distance as ld, soundex
print("This is starting : ",ld("anjan","anjana"))
"""
List of functions written for semantic type : 
1. check_park
2. check_agency
3. check_subject
4. check_street
5. check_address
6. check_schLvl
7. check_website
8. check_build_cls
9. check_zipcode
10. check_school_name
11. check_borough
12. check_phoneNum
13. check_color
14. check_city
15. check_latitude/longitude
16. check_biz
17. check_neigh
18. check_car_make
19. check_area_of_study
20. check_uni_name
21. check_body
22. check_loc
"""



def preprocess(df):
    df = df.na.fill("null")
    return df



# Code for location type check
def check_loc(name):
    name = name.lower()
    name = name.replace("&","")
    name = name.replace("and","")
    name = name.replace("/"," ")
    name = name.replace(".","")
    name= name.replace("-","")
    name = name.strip()
#     name = name.split(" ")
    lst = ["residence","house","bar","club","subway","diner","market","street",
           "store","hospital","grocery","bus"]
    for item in lst:
        if item in name:
            return "location_type"
    return "other"

    
check_loc = F.udf(check_loc, StringType())



# Code for vehicle body Validation
def check_body(name):
    name = name.strip()
    name = name.lower()
    name = name.replace("&","")
    name = name.replace("and","")
    name = name.replace("/"," ")
    name = name.split(" ")
    lst = ["wagon", "truck", "sedan", "bike","bus","pick","taxi",
           "van","cycle","cab","motor","door","moped","conv",
           "garbage","mixer","ambul","passenger","tank","flat","bed","wheel",
           "limo","vehicle","dump","train","delv","subn","dsd","util","refg","trlr","pkup","semi"]
    for n in name:
        for item in lst:
            if ld(n, item)<=2:
                return "vehicle_type"
    return "other"

    
check_body = F.udf(check_body, StringType())


# Code for university name Validation
def check_uni_name(name):
    name = name.strip()
    name = name.lower()
    name = name.replace(","," ")
    name = name.replace("&","")
    if name=="null":
        return "other"
    if "university" in name:
        return "college_name"
    if "college" in name and "school" not in name:
        return "college_name"
    return "other"


check_uni_name = F.udf(check_uni_name, StringType())

# Code for area of study validation
def check_area_of_study(name):
    if name=="null" or name=="-":
        return "other"
    name = name.strip()
    name = name.lower()
    name = name.replace("&","")
    name = name.replace("and","")
    name = name.replace("/"," ")
    name = name.split(" ")
    lst = ['general agriculture', 'agriculture production and management', 'agricultural economics', 'animal sciences', 'food science', 'plant science and agronomy', 'soil science', 'miscellaneous agriculture', 'forestry', 'natural resources management', 'fine arts', 'drama and theater arts', 'music', 'visual and performing arts', 'commercial art and graphic design', 'film video and photographic arts', 'studio arts', 'miscellaneous fine arts', 'environmental science', 'biology', 'biochemical sciences', 'botany', 'molecular biology', 'ecology', 'genetics', 'microbiology', 'pharmacology', 'physiology', 'zoology', 'neuroscience', 'miscellaneous biology', 'cognitive science and biopsychology', 'general business', 'accounting', 'actuarial science', 'business management and administration', 'operations logistics and e-commerce', 'business economics', 'marketing and marketing research', 'finance', 'human resources and personnel management', 'international business', 'hospitality management', 'management information systems and statistics', 'miscellaneous business & medical administration', 'communications', 'journalism', 'mass media', 'advertising and public relations', 'communication technologies', 'computer and information systems', 'computer programming and data processing', 'computer science', 'information sciences', 'computer administration management and security', 'computer networking and telecommunications', 'mathematics', 'applied mathematics', 'statistics and decision science', 'mathematics and computer science', 'general education', 'educational administration and supervision', 'school student counseling', 'elementary education', 'mathematics teacher education', 'physical and health education teaching', 'early childhood education', 'science and computer teacher education', 'secondary teacher education', 'special needs education', 'social science or history teacher education', 'teacher education: multiple levels', 'language and drama education', 'art and music education', 'miscellaneous education', 'library science', 'architecture', 'general engineering', 'aerospace engineering', 'biological engineering', 'architectural engineering', 'biomedical engineering', 'chemical engineering', 'civil engineering', 'computer engineering', 'electrical engineering', 'engineering mechanics physics and science', 'environmental engineering', 'geological and geophysical engineering', 'industrial and manufacturing engineering', 'materials engineering and materials science', 'mechanical engineering', 'metallurgical engineering', 'mining and mineral engineering', 'naval architecture and marine engineering', 'nuclear engineering', 'petroleum engineering', 'miscellaneous engineering', 'engineering technologies', 'engineering and industrial management', 'electrical engineering technology', 'industrial production technologies', 'mechanical engineering related technologies', 'miscellaneous engineering technologies', 'materials science', 'nutrition sciences', 'general medical and health services', 'communication disorders sciences and services', 'health and medical administrative services', 'medical assisting services', 'medical technologies technicians', 'health and medical preparatory programs', 'nursing', 'pharmacy pharmaceutical sciences and administration', 'treatment therapy professions', 'community and public health', 'miscellaneous health medical professions', 'area ethnic and civilization studies', 'linguistics and comparative language and literature', 'french german latin and other common foreign language studies', 'other foreign languages', 'english language and literature', 'composition and rhetoric', 'liberal arts', 'humanities', 'intercultural and international studies', 'philosophy and religious studies', 'theology and religious vocations', 'anthropology and archeology', 'art history and criticism', 'history', 'united states history', 'cosmetology services and culinary arts', 'family and consumer sciences', 'military technologies', 'physical fitness parks recreation and leisure', 'construction services', 'electrical', 'industrial arts & consumer services', 'transportation sciences and technologies', 'multi/interdisciplinary studies', 'court reporting', 'pre-law and legal studies', 'criminal justice and fire protection', 'public administration', 'public policy', 'physical sciences', 'astronomy and astrophysics', 'atmospheric sciences and meteorology', 'chemistry', 'geology and earth science', 'geosciences', 'oceanography', 'physics', 'multi-disciplinary or general science', 'nuclear', 'physical sciences', 'psychology', 'educational psychology', 'clinical psychology', 'counseling psychology', 'industrial and organizational psychology', 'social psychology', 'miscellaneous psychology', 'human services and community organization', 'social work', 'interdisciplinary social sciences', 'general social sciences', 'economics', 'criminology', 'geography', 'international relations', 'political science and government', 'sociology', 'miscellaneous social sciences']
    for nm in name:
        for item in lst:
            if nm == item:
                return "area_of_study"
    return "other"

    
check_area_of_study = F.udf(check_area_of_study, StringType())


# Code for car make validation
def check_car_make(name):
    name = name.strip()
    name = name.lower()
    name = name.replace("`","")
    name = name.replace("."," ")
    name = name.replace("0","0")
    lst = ["volvo","chrylsr","datsun","chevy","chevrolet","ford","volkswagen",
           "vw","buick","mercury","dodge","subaru","mazda","audi","honda","hyundai",
           "plymth","lincoln","toyota","renault","peugot","nissan","isuzu","cadillac",
            "yamaha", "jeep","saab","yamaha","porshe","oldsmob","pontac","ferrari","mitsubi",
            "eagle","jaguar","camaro"]
    if name=="-":
        return "other"
    for item in lst:
        if ld(name, item)<=2:
            return "car_make"
    return "other"
    
    if name == "null":
        return "other"
    for item in ls:
        if item in name:
            return "neighborhood"
    return "other"
    
check_make = F.udf(check_car_make, StringType())

# Code for neighborhood validation
def check_neighborhood(name):
    name = name.strip()
    name = name.lower()
    name = name.replace(".","")
    name = name.replace(","," ")
    name = name.split("-")[0]
    
    ls = ['albans','hollis hills', 'jamaica', 'borough park', 'laguardia airport', 
          'stapleton', 'flatiron district', 'stuyvesant town', 'clifton', 
          'westchester square', 'claremont village', 'claremont', 'central park', 
          'emerson hill', 'theater district', 'noho', 'laurelton', 
          'sunnyside', 'clinton hill', 'castle hill', 'ozone park', 
          'highbridge', 'canarsie', 'park slope', 'pelham bay park', 
          'bergen beach', 'upper east side', 'latourette park', 'soundview', 
          'governors island', 'kingsbridge', 'queens village', 'two bridges', 
          "hell's kitchen", 'bloomfield', 'briarwood', 'bay terrace, staten island', 
          'brighton beach', 'douglaston', 'tribeca', 'windsor terrace', 'bronx park', 
          'grymes hill', 'brooklyn heights', 'columbia st', 'concourse', 'ellis island', 
          'ditmars steinway', 'bedford','stuyvesant', 'kew gardens', 'east harlem', 
          'bronxdale', 'bayswater', 'bushwick', 'city island', 'huguenot', 'long island city',
          'new dorp beach', 'richmondtown', 'bellerose', 'east new york', 'maspeth',
          'country club', 'sheepshead bay', 'mariners harbor', 'arlington', 'new brighton',
          'greenwich village', 'westerleigh', 'mill basin', 'spuyten duyvil', 'van nest',
          'washington heights', 'sea gate', 'jamaica estates', 'plum beach', 'morrisania',
          'kips bay', 'tottenville', 'bay ridge', 'east morrisania', 'little neck', 'grant city', 
          'green-wood cemetery', 'west village', 'gerritsen beach', 'soho', 'alley pond park', 
          'lighthouse hill', 'oakwood', 'floral park', 'co-op city', 'cambria heights', 'chinatown', 
          'midtown', 'rockaway beach', 'breezy point', 'howard beach', 'arrochar', 'edenwald', 
          'prospect heights', 'south ozone park', 'marble hill', 'manhattan beach', 'upper west side', 
          'fort wadsworth', 'hunts point', 'roosevelt island', 'port richmond', 'morris heights', 
          'mount eden', 'jamaica bay', 'norwood', 'gramercy', 'fieldston', 'bath beach', 'rosedale', 
          'charleston', 'coney island', 'glendale', 'broad channel', 'cypress hills', 'randall manor', 
          'graniteville', 'west farms', 'civic center', 'concord', 'gowanus', 'mount hope', 'whitestone', 
          'dumbo', 'glen oaks', 'tremont', 'woodside', 'crotona park', 'richmond hill', 'rosebank', 'inwood', 
          'elmhurst', 'far rockaway', 'boerum hill', 'tompkinsville', "bull's head", 'cobble hill', 
          'concourse village', 'morris park', 'van cortlandt park', 'pleasant plains', 'neponsit', 'forest park', 
          'arverne', 'park hill', 'st. george', 'belle harbor', 'eastchester', 'prospect park', 
          'university heights', 'middle village', 'crown heights', 'sunset park', 'chelsea', 'pelham islands', 
          'todt hill', 'flatlands', 'port morris', 'springfield gardens', 'bay terrace', 
          'st. albans', 'dyker heights', 'dongan hills', 'battery park city', 'downtown brooklyn', 
          'howland hook', 'lower east side', 'rikers island', 'ridgewood', 'longwood', 'hart island',
          'pelham bay', 'wakefield', 'jackson heights', 'parkchester', 'pelham gardens', 'little italy', 
          'greenpoint', 'jamaica hills', 'fort greene', 'marine park', 'williamsburg', 'morningside heights', 
          'schuylerville', 'south brother island', 'kew gardens hills', 'midland beach',
          'shore acres', 'corona', 'south beach', 'rossville', 'hoffman island', 'john f. kennedy international airport',
          'midwood', 'east flatbush', 'flatbush', 'brownsville', 'vinegar hill', 'port ivory', 'fort hamilton', 
          'woodlawn', 'college point', 'fordham', 'great kills', 'north riverdale', 'throgs neck', 'mott haven', 
          'rockaway park', 'navy yard', 'liberty island', 'bensonhurst', 'castleton corners', 'great kills park', 
          "prince's bay", 'belmont', 'riverdale', 'willowbrook', "randall's island", 'east village', 'woodhaven', 
          'west brighton', 'chelsea, staten island', 'baychester', 'clason point', 'melrose', 'nolita', 
          'floyd bennett field', 'williamsbridge', 'cunningham park', 'gravesend', 'bayside', 'ferry point park', 
          'silver lake', 'financial district', 'forest hills', 'flushing meadows corona park', 'freshkills park', 
          'olinville', 'kensington', 'woodrow', 'arden heights', 'carroll gardens', 'fresh meadows', 'east elmhurst', 
          'hollis', 'prospect-lefferts gardens', 'south slope', 'rego park', 'unionport', 'eltingville', 'murray hill', 
          'holliswood', 'new dorp', 'new springville', 'red hook', 'allerton', 'harlem', 'flushing', 
          'north brother island', 'astoria', 'edgemere','lincoln square','clinton','manhattan','hunters point',
         'prospect lefferts gardens','Van Cortlandt Village']
    
    
    if name == "null":
        return "other"
    for item in ls:
        if item in name:
            return "neighborhood"
    return "other"
    
check_neigh = F.udf(check_neighborhood, StringType())


# Code for latitude and longitude validation
def check_latLong(number):
    def islat(num):
        if num>=-90 and num<=90:
            return True
        else:
            return False
    
    def islong(num):
        if num>=-180 and num<=180:
            return True
        else:
            return False
    
    number = number.replace('(', '')
    number = number.replace(')', '')
    lst = number.split(',')
    if len(lst)>1:
        try:
            lat = float(lst[0])
            long = float(lst[1])
            if islat(lat) or islong(long):
                return "lat_lon_cord"
            else:
                return "other"
        except:
            return "other"
    else:
        try:
            val = float(lst[0])
            if islat(val) or islong(val):
                return "lat_lon_cord"
            else:
                return "other"
        except:
            return "other"

check_lat_long = F.udf(check_latLong, StringType())


# Code for park Validataion
def check_park(word):
    word = word.strip()
    word = word.lower()
#     word = word.replace("."," ")
#     word = word.replace("-"," ")
#     word = word.replace("/"," ")
#     word = word.replace("("," ")
#     word = word.replace(")"," ")
    ls = word.split(" ")
    lst = ['parks', 'playground', 'gardens','picnic','recs','recreations','zoo', 'rink',
           'square','triangle','plgds','water','front','ferry', 'lake','field', 'center']
    for item in ls:
        for it in lst:
            if item in it:
                return "park_playground"
    return "other"
        
    
check_park = F.udf(check_park, StringType())

# Code for agency Validation
def check_agency(name):
    name = name.strip()
    name = name.lower()
    lst = ['nycem','nychh','dop','hra','doi','acs','dot','hpd','dca','dpr','ddc','tlc','dcp','dof','doris','cuny','oath','nypd',
           'lpc','dob','qpl','sca','ccrb','dfta','dycd','dcla','nypl','doitt','bic','law','dep','boe','doc','sbs','doe','dohmh',
           'dsny','bpl','dcas','dhs','311','nycha','cchr','fdny','edc','hhc','ocme','oem']
    if name=="null":
        return "other"
    if name in lst:
        return "city_agency"
    else:
        return "other"
    
check_agency = F.udf(check_agency, StringType())

# Code for subject Validation
def check_subject(name):
    name = name.strip()
    name = name.split(" ")
#     name = name.lower()
    ls = [n.lower() for n in name]
    lst = ['math', 'social', 'science', 'english', 'biology', 'history', 'geography', 'algebra', 'economics',
            'chemistry', 'geometry']
    if ls[0]=="null":
        return "other"
    for item in ls:
        for it in lst:
            if item in it:
                return "subject_in_school"
    return "other"
            
check_subject = F.udf(check_subject, StringType())            
            
# Code for street validation
def check_street(name):
    name = name.strip()
    name = name.split(" ")
    ls = [n.lower() for n in name]
    for item in ls:
        try:
            int(item)
            return "other"
        except:
            continue
    word = ls[-1]
    if word == "null":
        return "other"
    lst = ['boulevard', 'parkway', 'east', 'west', 'street', 'avenue', 'lane', 
           'place' , 'road', 'broadway', 'beach', 'drive', 'trail', 'circle', 'promenade','transit',
            'park', 'highway', 'expressway', 'parkway', 'overpass', 'tunnel', 'slip',
            'bridge', 'exit', 'loop', 'court', 'ramp', 'alley', 'entrance', 'heights', 'oval']
    slst = [soundex(l) for l in lst]
    if any(item in ls for item in lst):
        return "street_name"
    elif any(soundex(item) in ls for item in slst):
        return "street_name"
    else:
        return "other"
    
check_street = F.udf(check_street, StringType())

# Code for address validation
def check_address(word):
    word = word.strip()
    word = word.lower()
    word = word.replace(" ","")
    word = word.replace(",","")
    if word == "null":
        return "other"
    try:
        int(word)
        return "other"
    except:
        None
    if re.match("\d+\w+\d*", word):
        return "address"
    return "other"
        
    
check_address = F.udf(check_address, StringType())


# Code for schoolLevel Validation
def check_schLvl(word):
    word = word.strip()
    word = word.lower()
    if word == "null":
        return "other"
    lst = ['elementary', 'middle', 'k-2', 'k-3', 'k-8', 'high school' , 'high school transfer', 'yabc']
    if word in lst:
        return "school_level"        
    else:
        return "other"
                
check_school_level = F.udf(check_schLvl, StringType())


# Code for website Validation
def check_website(word):
    if word=='null':
        return "other"
    ls = re.split('\.|\/',word)
    lst = ['gov', 'com', 'org', 'info', 'http', 'https', 'net','edu','www']
    if any(item in ls for item in lst):
        return "website"
    else:
        return "other"

check_website = F.udf(check_website, StringType())






# Code for building classification
def check_build_cls(word):
    if '-' not in word:
        return "other"
    if word == "null":
        return "other"
    word = word.strip().split("-")[0]
    word = word.upper()
    lst = ["A0","A1","A2","A3","A4","A5","A6","A7","A8","A9",
           "B1","B2","B3","B9","C0","C1","C2","C3","C4","C5","C6","C7","C8","C9","CM",
           "D0","D1","D2","D3","D4","D5","D6","D7","D8","D9",
           "E1","E2","E3","E4","E7","E9","F1","F2","F4","F5","F8","F9",
           "G0","G1","G2","G3","G4","G5","G6","G7","G8","G9","GU","GW","G9",
           "HB","HH","HR","HS","H1","H2","H3","H4","H5","H6","H7","H8","H9",
           "I1","I2","I3","I4","I5","I6","I7","I9","J1","J2","J3","J4","J5","J6","J7","J8","J9",
           "K1","K2","K3","K4","K5","K6","K7","K8","K9","L1","L2","L3","L8","L9",
           "M1","M2","M3","M4","M9","N1","N2","N3","N4","N9",
           "O1","O2","O3","O4","O5","O6","O7","O8","O9","P1","P2","P3","P4","P5","P6","P7","P8","P9"
           ,"Q1","Q2","Q3","Q4","Q5","Q6","Q7","Q8","Q9",
           "RA","RB","RG","RH","RK","RP","RR","RS","RT","RW","R0","R1","R2","R3","R4","R5","R6","R7","R8","R9","RR",
           "S0","S1","S2","S3","S4","S5","S9","T1","T2","T9",
           "U0","U1","U2","U3","U4","U5","U6","U7","U8","U9","V0","V1","V2","V3","V4","V5","V6","V7","V8","V9",
           "W1","W2","W3","W4","W5","W6","W7","W8","W9","Y1","Y2","Y3","Y4","Y5","Y6","Y7","Y8","Y9",
           "Z0","Z1","Z2","Z3","Z4","Z5","Z7","Z8","Z9"]
    if word in lst:
        return "building_classification"        
    else:
        return "other"
                
check_build_cls = F.udf(check_build_cls, StringType())


# Code for business Name validation
def check_bizName(name):
    name = name.strip()
    name = name.lower()
    name = name.replace(".","")
    ls = ["co","resto","restaurant","bistro","llc","ltd","inc","pizza","bar",
           "cafe","donuts","grill", "beer","little caesars", "pret","kitchen","hotel","tavern",
            "diner","pllc","group","solutions","pc","pe",'llp','ra']
    ls_new = ["donut","pizz","food","bar","deli","house","bbq","corp","engineer",
              'associate',"architect","consult","service","design","studio"]
    lst = name.split(" ")
    if lst[0]=="the" or lst[0]=='le' or lst[0]=='la':
        return "business_name"
    if name == "null":
        return "other"
    if re.search("\w+\s*[a][n][d]\s*\w+", name):
        return "business_name"
    elif re.search("\s[b][y]\s", name):
        return "business_name"
    elif re.search("\w'[s]", name):
        return "business_name"
    elif any(item in ls for item in lst):
        return "business_name"
    elif any("burger" in item for item in lst):
        return "business_name"
    elif any(it in item for it in ls_new for item in lst):
        return "business_name"
    else:
        return "other"
    
check_biz = F.udf(check_bizName, StringType())



# Code for zipcode validation
def check_zipCode(number):
    number = number.strip()
    if number == "null":
        return "other"
    
    zip_re = re.compile('^[0-9]{5}(?:-[0-9]{4})?$') #accepts 11209 as well as 11209-1234
    if not zip_re.match(number):
        return "other"
    
    code = number[:5]
    if int(code)<10000 or int(code)>19999:
        return "other"
    return "zipcode"    
    
check_zipCode = F.udf(check_zipCode, StringType())


def check_school_name(name):
    name = name.strip()
    name = name.replace(" ","")
    name = name.replace("/","")
    name = name.replace(".","")
    name = name.lower()
    lst = ["ps","i.s","hs","high","school","academy","ms"]
    if any(item in name for item in lst):
        return "school_name"
    else:
        return "other"
    
check_sch_name = F.udf(check_school_name, StringType())



# Code for borough checking. RUN THIS AND READ A FILE AND CALL json_for_borough(dataframe, original_col_name)
def check_borough(word):
    word = word.lower()
    if word in ["manhattan", "brooklyn", "bronx", "staten island", "queens"]:
        return "borough"
    else:
        return "other"

check_borough = F.udf(check_borough, StringType())


# Code for phone checking. RUN THIS AND READ A FILE AND CALL json_for_phone(dataframe, original_col_name)
def check_phoneNum(number):
    number = number.strip()
    number = number.replace("-","")
    number = number.replace(".","")
    if number == 'null':
        return "other"
    if len(number)!= 10:
        return "other"
    if not number.isdigit():
        return "other"
        
    return "phone_number"
        
check_phoneNum = F.udf(check_phoneNum, StringType())


# Code for color Validation
lclr=['lime', 'lavender', 'clear', 'lm', 'q', 
      'violet', 'trns', 'red', 'rng', 'slvr', 
      'grn', 'copper', 'unknown', 'blck', 'aqua', 
      'trans', 'purple', 'bl', 'gry', 'yllw', 'wht', 'gld', 
      'azure', 'lvndr', 'zr', 'pq', 'tan', 'lns', 'flesh', 
      'magenta', 'yellow', 'cppr', 'orange', 'slmn', 'clr]', 
      'silver', 'opaque', 'rd', 'rust', 'tn', 'brwn', 'pink', 
      'gold', 'clr', 'flsh', 'pnk', 'llc', 'brown', 'black', 'turquoise', 
      'nknwn', 'gray', 'rst', 'color]', 'prpl', 'mgnt', 'brss', 'salmon', 
      'blue', 'green', 'trqs', 'vlt', 'lilac', 'white', 'lens', 'brass']


def check_color(word, lclr):
    word = word.strip()
    word = word.lower()
    if word == "null":
        return "other"
    if word == '-':
        return "other"
    for item in lclr:
        if ld(word, item)<=1:
            return "color"

    return "other"
                
check_color_l=F.udf(lambda x: check_color(x, lclr), StringType())




# Code for city Validation
citydf = spark.read.option("sep", "|").option("header", "true").csv("/user/aks740/us_cities_states_counties.csv")
citydf = citydf.filter(citydf["State short"]=="NY")
mvv = citydf.select("City").rdd.flatMap(lambda x: x).collect()
lowermvv = [c.lower() for c in mvv]
def check_city(word, mvv):
    if word is None:
        return "other"
    word = word.strip()
    word = word.lower()
    if word == "null":
        return "other"
    lst = mvv
    if word in lst:
        return "city"        
    else:
        return "other"
                
check_city_l=F.udf(lambda x: check_city(x,lowermvv), StringType())


# Code for name check
with open("male.txt") as f:
    male = f.readlines()
with open("female.txt") as f:
    female = f.readlines()
male = [x.strip() for x in male]
female = [x.strip() for x in female]
content = male+female


def check_name(name, content):
    if name=="null" or name=="-":
        return "other"
    name = name.lower()
    name = name.replace("&","")
    name = name.replace("and","")
    name = name.replace("/"," ")
    name = name.replace("-","")
    name = name.strip()
    name = name.split(",")
    if len(name[0])==1:
        return "person_name"
    for nm in content:
        for n in name:
            n = n.strip()
            if ld(n,nm)<=3:
                return "person_name"
    return "other"

    
check_name_l = F.udf(lambda x: check_name(x, content), StringType())


"""
def check_name(name, content):
    if name=="null" or name=="-":
        return "other"
    name = name.lower()
    name = name.replace("&","")
    name = name.replace("and","")
    name = name.replace("/"," ")
    name = name.replace(".","")
    name= name.replace("-","")
    name = name.strip()
    if len(name)==1:
        return "person_name"
    for nm in content:
        if soundex(name) in soundex(nm):
            return "person_name"
    return "other"
#     name = name.split(" ")
    lst = ["residence","house","bar","club","subway","diner","market","street",
           "store","hospital","grocery","bus"]
    for item in lst:
        if item in name:
            return "location_type"
    return "other"

    
check_name_l = F.udf(lambda x: check_name(x, content), StringType())
"""


"""
For each entry in the cluster2.txt file
1. Read the filename.
2. Check if the column exists.
3. Call check_1 and call the colm check_1. Find count of proper/other
4. Do the above for all the check_i.
5. Return the type that has least Other.
6. Find count of nulls where check_i is null and check_i+1 is null and so on.
"""

mapping = {
    "check1":"website",
    "check2":"borough",
    "check3":"phone_number",
    "check4":"building_classification",
    "check5":"city",
    "check6":"school_level",
    "check7":"school_name",
    "check8":"color",
    "check9":"address",
    "check10":"street_name",
    "check11":"subject_in_school",
    "check12":"city_agency",
    "check13":"park_playground",
    "check14":"zip_code",
    "check15":"lat_lon_cord",
    "check16":"business_name",
    "check17":"neighborhood",
    "check18":"car_make",
    "check19":"area_of_study",
    "check20":"college_name",
    "check21":"vehicle_type",
    "check22":"location_type",
    "check23":"person_name"
}
    
def find_type(df, col):
    df = preprocess(df)
    df = df.withColumn("check1", check_website(F.col(col)))
    df = df.withColumn("check2", check_borough(F.col(col)))
    df = df.withColumn("check3", check_phoneNum(F.col(col)))
    df = df.withColumn("check4", check_build_cls(F.col(col)))
    df = df.withColumn("check5", check_city_l(F.col(col)))
    df = df.withColumn("check6", check_school_level(F.col(col)))
    df = df.withColumn("check7", check_sch_name(F.col(col)))
    df = df.withColumn("check8", check_color_l(F.col(col)))
    df = df.withColumn("check9", check_address(F.col(col)))
    df = df.withColumn("check10", check_street(F.col(col)))
    df = df.withColumn("check11", check_subject(F.col(col)))
    df = df.withColumn("check12", check_agency(F.col(col)))
    df = df.withColumn("check13", check_park(F.col(col)))
    df = df.withColumn("check14", check_zipCode(F.col(col)))
    df = df.withColumn("check15", check_lat_long(F.col(col)))
    df = df.withColumn("check16", check_biz(F.col(col)))
    df = df.withColumn("check17", check_neigh(F.col(col)))
    df = df.withColumn("check18", check_make(F.col(col)))
    df = df.withColumn("check19", check_area_of_study(F.col(col)))
    df = df.withColumn("check20", check_uni_name(F.col(col)))
    df = df.withColumn("check21", check_body(F.col(col)))
    df = df.withColumn("check22", check_loc(F.col(col)))
    df = df.withColumn("check23", check_name_l(F.col(col)))
    #df.filter(df.check11!='other').select(col, "check11").show()
    countr = []
    for i in range(1,24):
        colname = "check"+str(i)
        countr.append(return_ct(df, colname))
#     print(countr)
#     sorted(countr, key=lambda x:x[0])
    countr.sort()
#     print("This is counter:",countr)
    jsonval =  {
        "column_name": col,
        "semantic_types": []
    }
    for item in countr:
        jsonToInsert = {
            "semantic_type":item[1][0],
            "label":countr[0][1][0],
            "count":item[1][1]
        }
        jsonval["semantic_types"].append(jsonToInsert)
    jsonToInsert = {
        "semantic_type":"other",
        "label":countr[0][1][0],
        "count" : otherVal(df)
    }
    jsonval["semantic_types"].append(jsonToInsert)
    return jsonval






def otherVal(df):
    df.createOrReplaceTempView("df")
    query = "select count(*) as ctr from df where check1=='other' and check2=='other' and \
    check3=='other' and check4=='other' and check5=='other' and check6=='other' \
    and check7=='other' and check8=='other' and check9=='other' and check10=='other' \
    and check11=='other' and check12=='other' and check13=='other' \
    and check14=='other' and check15=='other' and check16=='other' \
    and check17=='other' and check18=='other' and check19=='other' and check20=='other' \
    and check21=='other' and check22=='other' and check23=='other'"
    temp = spark.sql(query)
    for row in temp.rdd.collect():
        return row.ctr
    
def return_ct(df, colname):
    df.createOrReplaceTempView("df")
    query = "select "+colname+", count(*) as ctr from df group by "+colname
    temp = spark.sql(query)
#     temp.show()
    print(query)
    tm = [0,0]
    flag=True
    for row in temp.rdd.collect():
        if row[colname]=="other":
            tm[0] = row.ctr
        else:
            flag=False
            tm[1] = (row[colname], row.ctr)
    if flag:
        tm[1] = (mapping[colname], 0)
    return tm



with open('cluster2.txt') as fp:
    data = [line for line in fp]
data = " "+data[0][1:-1]
# print(data)
data = data.split(",")
passVal = []
for row in data:
    l = row.split(".")
    passVal.append([l[0][2:]+".tsv.gz", l[1]])

def process_string(string):
    for ch in [" " , "." , "," , "(" , ")" , "/" , "*" , "–" , "’" , "'" , "#"]:
        string = string.replace(ch, "_")
    return string


for item in passVal[:50]:
    filename = "/user/hm74/NYCOpenData/"+item[0]
    tempdf = spark.read.option("sep","\t").option("header", "true").csv(filename)
    #print(tempdf.columns[3])
    tempdf = tempdf.toDF(*(process_string(c) for c in tempdf.columns))
    #tempdf = tempdf.toDF(*(re.sub(r'[/]+', '_', c) for c in tempdf.columns))
    if item[1] not in tempdf.columns:
         print(filename, "not processed")
         continue
    #print(tempdf.columns[3])
    if tempdf.count() <= 100000:
        print("File being processed : ",filename)
        jsonfile = "./task2json/"+item[0]+".json"
        with open(jsonfile, 'w+', encoding='utf-8') as f:
            json.dump(find_type(tempdf, item[1]), f, ensure_ascii=False, indent=4)
        #print("The predicted type for ",item[1],"is",find_type(tempdf, item[1]))
    #print("File is : ",item[0], item[1])


#tempdf = spark.read.option("sep", "\t").option("header", "true").csv("/user/hm74/NYCOpenData/s3k6-pzi2.tsv.gz")
#print("The predicted semantic type of website is : ",find_type(tempdf, "website"))
print("***********TASKS DONE******************")


