# Databricks notebook source
#Ethan Engel
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType,LongType,TimestampType
fifaSchema = StructType( \
 [StructField('ID', LongType(), True), \
 StructField('lang', StringType(), True), \
 StructField('Date', TimestampType(), True), \
 StructField('Source', StringType(), True), \
 StructField('len', LongType(), True), \
 StructField('Orig_Tweet', StringType(), True), \
 StructField('Tweet', StringType(), True), \
 StructField('Likes', LongType(), True), \
 StructField('RTs', LongType(), True), \
 StructField('Hashtags', StringType(), True), \
 StructField('UserMentionNames', StringType(), True), \
 StructField('UserMentionID', StringType(), True), \
 StructField('Name', StringType(), True), \
 StructField('Place', StringType(), True), \
 StructField('Followers', LongType(), True), \
 StructField('Friends', LongType(), True), \
 ])

fifa_df=spark.read.format("csv").option("header","true").option('escape','"').schema(fifaSchema).load("dbfs:///FileStore/tables/lab8/FIFA.csv")
trimmed_df=fifa_df.drop("lang","Source","len","Orig_Tweet","Tweet",\
"Likes","RTs","UserMentionNames","UserMentionID","Name","Place","Followers","Friends")
hashtag_df=trimmed_df.filter(trimmed_df.Hashtags.isNotNull())
hashtag_df = hashtag_df.withColumn('Hashtags', f.explode(f.split('Hashtags', ',')))

#hashtag_df.createOrReplaceTempView('fifa_table')

static_window=hashtag_df.groupBy(f.window("Date","60 minutes","30 Minutes"),"Hashtags").agg(f.count("Hashtags")).filter(f.count("Hashtags") > 100).orderBy("window","count(Hashtags)").show(truncate=False)


# COMMAND ----------

dbutils.fs.rm("FileStore/tables/lab8partitions/", True)
fifa_df=fifa_df.repartition(60)
print(fifa_df.rdd.getNumPartitions())
fifa_df.write.format("csv").option("header",True).save("FileStore/tables/lab8partitions/")

# COMMAND ----------

fifastream_df=spark.readStream.format("csv").option("header","true").option('escape','"').option("maxFilesPerTrigger",1).schema(fifaSchema).load("dbfs:///FileStore/tables/lab8partitions/")
trimmedstream_df=fifastream_df.drop("lang","Source","len","Orig_Tweet","Tweet",\
"Likes","RTs","UserMentionNames","UserMentionID","Name","Place","Followers","Friends")
hashtagstream_df=trimmedstream_df.filter(trimmedstream_df.Hashtags.isNotNull())
hashtagstream_df = hashtagstream_df.withColumn('Hashtags', f.explode(f.split('Hashtags', ',')))

sinkStream=hashtagstream_df.writeStream.outputMode("update").format("memory").queryName("hashtotals").trigger(processingTime='20 seconds').start()


# COMMAND ----------

spark.sql("SELECT Hashtags, COUNT(Hashtags) FROM hashtotals GROUP BY Hashtags").withWatermark("Date","24 hours").show(10)
