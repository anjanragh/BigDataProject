from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import date_format, format_string, desc,explode,regexp_extract,col,length,avg,max,min,count
from pyspark.sql.types import LongType, DoubleType,StringType,DateType
import datetime as dt,json,sys,subprocess

def readDirectory(path):
  args = "hdfs dfs -ls "+path+" | awk '{print $8}'"

  proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)

  s_output, s_err = proc.communicate()
  all_dart_dirs = s_output.split()

  filesToRead = []

  for f in all_dart_dirs:
    filesToRead.append(f.decode("utf-8"))

  return filesToRead
   
def int_stats(df, col):
    tmp = df.withColumn("intval", df[col].cast(LongType()))
    tmp = tmp.filter(tmp.intval.isNotNull())
    stats = tmp.agg(F.min(tmp.intval).alias('min'),F.max(tmp.intval).alias('max'),F.avg(tmp.intval).alias('avg'),F.stddev(tmp.intval).alias('stddev'), F.count(tmp.intval).alias('cnt'))
    return tmp.select(col),stats

def real_stats(df, col):
    tmp = df.withColumn("realval", df[col].cast(DoubleType()))
    tmp = tmp.filter(tmp.realval.isNotNull())
    stats = tmp.agg(F.min(tmp.realval).alias('min'),F.max(tmp.realval).alias('max'),F.avg(tmp.realval).alias('avg'),F.stddev(tmp.realval).alias('stddev'), F.count(tmp.realval).alias('cnt'))
    return tmp.select(col),stats
    
def date_time_stats(df, col):
    tmp = df.withColumn("dateTime", df[col].cast(DateType()))
    tmp = tmp.filter(tmp.dateTime.isNotNull())
    stats = tmp.agg(F.min(tmp.dateTime).alias('min'),F.max(tmp.dateTime).alias('max'), F.count(tmp.dateTime).alias('cnt'))
    return tmp.select(col),stats
    
def text_stats(df, col):
    # Convert the column into string here. 
    tmp = df.withColumn("textVal", df[col].cast(StringType()))
    tmp = df.withColumn("length", F.length(F.col(col)))
    tmp = tmp.filter(tmp.length.isNotNull())
    stats = tmp.agg(F.count(tmp.length).alias("cnt"),F.avg(tmp.length).alias("avg")).collect()
    tmp.createOrReplaceTempView("tmp")
    queryasc = "select `"+col+"` , length from tmp order by length limit 5"
    querydesc = "select `"+col+"` , length from tmp order by length desc limit 5"
    asc_stats = spark.sql(queryasc).collect()
    dsc_stats = spark.sql(querydesc).collect()
    return stats,asc_stats,dsc_stats

def null_stats(df, col):
    tmp = df.filter(df[col].isNull())
    return tmp

 
def processData(dataset,ifile):
    totalRows = dataset.count()
    if totalRows < 6999999:
        return None
    output = {"start time":str(dt.datetime.now())}
    output["row_count"]=totalRows
    output["column_count"]=len(dataset.columns)
    key_column_candidates = {}
    coldetails={}
    columns=[]

    for colm in dataset.columns:
        print(colm,str(dt.datetime.now()))
        # select only colm from dataset
        df = dataset.select(colm)
        # basic info about column
        coldetails["column_name"]=colm

        # frequent values
        freqVal = df.groupBy(colm).count().sort(col(colm).desc()).take(5)
        coldetails["frequent_values"]=[x[colm] for x in freqVal]

        df = dataset.select(colm).filter(dataset[colm].isNotNull())
        nullDf = null_stats(df,colm)
        nullValuesCount = nullDf.count()
        noOfDistinctValues = df.select(colm).distinct().count()

        # keyColumnCandidate extra credit :)
        diff = totalRows - noOfDistinctValues
        if diff in key_column_candidates:
            temp = key_column_candidates.get(diff)
            temp.append(colm)
            key_column_candidates[diff]=temp
        else:
            key_column_candidates[diff]=[colm]


        coldetails["number_non_empty_cells"]=totalRows-nullValuesCount
        coldetails["number_empty_cells"]=nullValuesCount
        coldetails["number_distinct_values"]=noOfDistinctValues

        # data-type info about column
        dataTypeInfo = []
        # solve this.
        # all values in column is null, hence continue with other columns.
        #if nullValuesCount == totalRows:
         #   coldetails["data_types"] = dataTypeInfo
          #  continue

        if totalRows-nullValuesCount > 0:
            # int values info
            # we can subtract the dataframe of null values to speed up and reducing size of df
            df = df.subtract(nullDf)
            intInfo = int_stats(df, colm)
            intDf, intStats = intInfo[0], intInfo[1].collect()
            if intStats[0].cnt > 0:
                intJson = {"type" : "Integer",
                           "count": intStats[0].cnt,
                           "max_val": intStats[0].max,
                           "min_val": intStats[0].min,
                           "mean": intStats[0].avg,
                           "stddev": intStats[0].stddev}
                dataTypeInfo.append(intJson)

            if totalRows-nullValuesCount-intStats[0].cnt > 0:
                # real values info
                # we can subtract the dataframe of null values to speed up and reducing size of df
                df = df.subtract(intDf)
                realInfo = real_stats(df, colm)
                realDf, realStats = realInfo[0], realInfo[1].collect()
                if realStats[0].cnt > 0:
                    realJson = {"type" : "Real",
                                "count": realStats[0].cnt,
                                "max_val": realStats[0].max,
                                "min_val": realStats[0].min,
                                "mean": realStats[0].avg,
                                "stddev": realStats[0].stddev}
                    dataTypeInfo.append(realJson)

                if totalRows-nullValuesCount-intStats[0].cnt-realStats[0].cnt > 0:
                    # date-time values info
                    # we can subtract the dataframe of null values to speed up and reducing size of df
                    df = df.subtract(realDf)
                    dateInfo = date_time_stats(df, colm)
                    dateDf, dateStats = dateInfo[0], dateInfo[1].collect()
                    if dateStats[0].cnt > 0:
                        dateJson = {"type" : "Date/Time",
                                    "count": dateStats[0].cnt,
                                    "max_val": dateStats[0].max,
                                    "min_val": dateStats[0].min}
                        dataTypeInfo.append(dateJson)

                    if totalRows-nullValuesCount-intStats[0].cnt-realStats[0].cnt-dateStats[0].cnt > 0:
                        # text value info
                        df = df.subtract(dateDf)
                        textInfo = text_stats(df, colm)
                        textStats, shortStats,longStats = textInfo[0], textInfo[1], textInfo[2]
                        if textStats[0].cnt > 0:
                            textJson = {"type" : "Text",
                                        "count": textStats[0].cnt,
                                        "shortest_values": [x[colm] for x in shortStats],
                                        "longest_values": [x[colm] for x in longStats],
                                        "average_length": textStats[0].avg}
                            dataTypeInfo.append(textJson)


        coldetails["dataTypes"]=dataTypeInfo
        columns.append(coldetails)

    for i in sorted(key_column_candidates.keys()) :
        output["key_column_candidates"]=key_column_candidates.get(i)
        break

    output["dataset_name"]=ifile.split('/')[-1]
    output["columns"]=columns
    output["end_time"]=str(dt.datetime.now())

    with open("largeDataOutput/"+ifile.split('/')[-1]+".json","w") as outfile:
        json.dump(output, outfile)

    print("finished")


st = str(dt.datetime.now())

ifiles = readDirectory(sys.argv[1])

spark = SparkSession \
    .builder \
    .appName("Big Data Project") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

fileCount = 1
for fileName in ifiles:
   try:
       print('started execution for ',fileName.split('/')[-1], ' at ',str(dt.datetime.now()),' with file count = ',fileCount)
       fileCount=fileCount+1
       dataset = spark.read.option("delimiter", "\t").option("header", "true").csv(fileName)
       processData(dataset,fileName)
       print('ended execution for ',fileName.split('/')[-1], ' at ',str(dt.datetime.now()))
   except:
       pass
et = str(dt.datetime.now())

print("execution started at ",st ,"and ended at ",et)
