
from pyspark import findspark
from pyspark.sql import SparkContext
import SparkSession 
findspark.init() 
sc = SparkContext(appName="MyFirstApp") 
spark = SparkSession(sc) 
print("Hello World!") 
sc.close() #closing the spark session
