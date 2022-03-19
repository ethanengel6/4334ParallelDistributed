# Databricks notebook source
dbutils.fs.mkdirs("FileStore/tables/lab3short")
dbutils.fs.cp("dbfs:///FileStore/tables/shortLab3data0-2.txt", "FileStore/tables/lab3short")
dbutils.fs.cp("dbfs:///FileStore/tables/shortLab3data1-3.txt", "FileStore/tables/lab3short")





# COMMAND ----------

sc = spark.sparkContext

url1 = sc.textFile("dbfs:///FileStore/tables/shortLab3data1-3.txt")
url2 = sc.textFile("dbfs:///FileStore/tables/shortLab3data0-2.txt")
url3=url1+url2

def splitter(str):
    return str.split()

pairs = url3.map(lambda x: (x.split(" ",1)[0], x.split(" ",1)[1]))

splitPairs= pairs.flatMapValues(splitter).sortByKey()

rddSwitched = splitPairs.map(lambda x: (x[1], x[0]))

groupedRdd=rddSwitched.groupByKey().map(lambda x : (x[0], list(x[1]))).sortByKey()
print(groupedRdd.collect())


# COMMAND ----------

sc = spark.sparkContext

url1A = sc.textFile("dbfs:///FileStore/tables/fullLab3data0-1.txt")
url2A = sc.textFile("dbfs:///FileStore/tables/fullLab3data1-1.txt")
url3A = sc.textFile("dbfs:///FileStore/tables/fullLab3data2-1.txt")
url4A = sc.textFile("dbfs:///FileStore/tables/fullLab3data3-1.txt")
url5A=url1A+url2A+url3A+url4A

def splitter(str):
    return str.split()

pairs2 = url5A.map(lambda x: (x.split(" ",1)[0], x.split(" ",1)[1]))

splitPairs2= pairs2.flatMapValues(splitter).sortByKey()

rddSwitched2 = splitPairs2.map(lambda x: (x[1], x[0]))

groupedRdd2=rddSwitched2.groupByKey().map(lambda x : (x[0], list(x[1]))).sortByKey()
print(groupedRdd2.take(10))
print(groupedRdd2.count())

