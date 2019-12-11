import json,operator,sys,re,math, statistics,datetime, numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, format_string, desc,explode,regexp_extract,col,length,avg,max,min
from dateutil.parser import parse

def is_date(s):
    try:
        parse(s)
        return True
    except ValueError:
        return False


def dtype(x):
    if re.match('^[-+]?[0-9]+',x):
        s=x.split('.')
        if(len(s))==1:
            try: 
                int(x)
                return "Integer"
            except ValueError:
                return "Text"
        elif len(s)==2:
            return "Real"
    if is_date(x):
        return "Date/Time"
    else:
        return "Text"

def isInt(x):
    try:
        int(x)
        return True
    except ValueError:
        return False

def isFloat(x):
    try:
        float(x)
        return True
    except ValueError:
        return False


spark = SparkSession \
    .builder \
    .appName("Big Data Project") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()


ifile = sys.argv[1]

dataset = spark.read.option("delimiter", "\t").option("header", "true").csv(ifile)

output = {"start time":str(datetime.datetime.now())}
columns = []
totalRows = dataset.count()
key_column_candidates = {}
count=1
for colm in dataset.columns:
    print(colm,count)
    count=count+1
    coldetails = {}
    dataTypes = {"Integer": False, "Real": False, "Date/Time": False, "Text": False}
    emptyCells = dataset.filter(dataset[colm].isNull()).count()
    colName = str(colm)
    coldetails["column_name"]=colName
    coldetails["number_non_empty_cells"]=totalRows-emptyCells
    coldetails["number_empty_cells"]=emptyCells
    allValues = dataset.select(colm)
    
    dataTypeInfo = []
    dataTypeCount = 0
    integerDS, realDS, dateTimeDS, textDS, lenTextDS,freqValue = [],[],[],[],0,{}
    for x in allValues.collect():
        if x is None:
            continue
        x=str(x[colName])
        if x in freqValue:
            freqValue[x] += 1
        else:
            freqValue[x]=1
        if isInt(x):
            integerDS.append(int(x))
            if dataTypes.get("Integer") == False:
                dataTypes["Integer"] = True
                dataTypeCount+=1
            continue
        elif isFloat(x):
            realDS.append(float(x))
            if dataTypes.get("Real") == False:
                dataTypes["Real"] = True
                dataTypeCount+=1
            continue
        elif is_date(x):
            dateTimeDS.append(parse(x))
            if dataTypes.get("Date/Time") == False:
                dataTypes["Date/Time"] = True
                dataTypeCount+=1
            continue
        else:
            textDS.append(x)
            lenTextDS = lenTextDS+len(x)
            if dataTypes.get("Text") == False:
                dataTypes["Text"] = True
                dataTypeCount+=1
            continue
            
    ## int info
    if len(integerDS) > 0:
        integerDS.sort()
        temp = {"type":"Integer"}
        temp["count"] = len(integerDS)
        temp["max_val"] = integerDS[-1]
        temp["min_val"] = integerDS[0]
        temp["mean"] = statistics.mean(integerDS)
        if len(integerDS)<2:
            temp["std_dev"] = "Cannot compute variance with only one datapoint"
        else:
            temp["std_dev"] = statistics.stdev(integerDS)
        dataTypeInfo.append(temp)

    ## real info
    if len(realDS) > 0:
        realDS.sort()
        temp = {"type":"Real"}
        temp["count"] = len(realDS)
        temp["max_val"] = realDS[-1]
        temp["min_val"] = realDS[0]
        temp["mean"] = statistics.mean(realDS)
        if len(realDS)<2:
            temp["std_dev"] = "Cannot compute variance with only one datapoint"
        else:
            temp["std_dev"] = statistics.stdev(realDS)
        dataTypeInfo.append(temp)

    ## text info
    if len(textDS) > 0:
        textDS = sorted(textDS, key=len) 
        temp = {"type":"Text"}
        temp["count"] = len(textDS)
        x = np.array(textDS)
        temp["longest_values"] = sorted(list(np.unique(textDS)), key=len, reverse = True)[:5]
        temp["shortest_values"] = sorted(list(np.unique(textDS)), key=len)[:5]
        temp["average_length"] = lenTextDS/len(textDS)
        dataTypeInfo.append(temp)
        
    ## date info
    if len(dateTimeDS) > 0:
        dateTimeDS.sort()
        temp = {"type":"Date/Time"}
        temp["count"] = len(dateTimeDS)
        temp["max_value"] = str(dateTimeDS[-1])
        temp["min_value"] = str(dateTimeDS[0])
        dataTypeInfo.append(temp)
    
    
    ###extra credit part: key_column_candidates.

    noOfDistinctValues = len(set(integerDS))+len(set(realDS))+len(set(textDS))+len(set(dateTimeDS))
    coldetails["number_distinct_values"]=noOfDistinctValues
    diff = totalRows - noOfDistinctValues
    if diff in key_column_candidates:
        temp = key_column_candidates.get(diff)
        temp.append(colName)
        key_column_candidates[diff]=temp
    else:
        key_column_candidates[diff]=[colName]
    sortedFrequentValues = sorted(freqValue.items(), key=operator.itemgetter(1),reverse=True)
    key,val = zip(*sortedFrequentValues)
    coldetails["frequent_values"] = key[:5]
    coldetails["dataTypes"]=dataTypeInfo
    columns.append(coldetails)

output["dataset_name"]=ifile
output["columns"]=columns

for i in sorted(key_column_candidates.keys()) :
    output["key_column_candidates"]=key_column_candidates.get(i)
    break
    
output["end time"]=str(datetime.datetime.now())
with open("outp311.json","w") as outfile:
     json.dump(output, outfile)

print("finished")
