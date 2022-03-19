# Databricks notebook source
dbutils.fs.mkdirs("FileStore/tables/lab6")

# COMMAND ----------

#Ethan Engel Lab6
from pyspark.sql.functions import col
allstar_df=spark.read.format("csv").option("header", "true").option("inferScema",True).load("dbfs:///FileStore/tables/lab6/AllstarFull.csv")
teams_df=spark.read.format("csv").option("header", "true").option("inferScema",True).load("dbfs:///FileStore/tables/lab6/Teams.csv")
master_df=spark.read.format("csv").option("header", "true").option("inferScema",True).load("dbfs:///FileStore/tables/lab6/baseball_master.csv")

allstar_df2=allstar_df.select(col("playerID"),col("teamID"))
teams_df2=teams_df.select(col("teamID"),col("name")).withColumnRenamed("name","teamName")
master_df2=master_df.select(col("playerID"),col("nameFirst"),col("nameLast"))
join1_df=allstar_df2.join(master_df2,["playerID"])
join2_df=join1_df.join(teams_df2,["teamID"])

join2_df.write.format("parquet").save("dbfs:///FileStore/tables/lab6/final_joined.parquet")

# COMMAND ----------


query_df=spark.read.format("parquet").load("dbfs:///FileStore/tables/lab6/final_joined.parquet")
rockies_df=query_df.select("nameFirst","nameLast",'teamName').filter(query_df["teamName"]=="Colorado Rockies")
print(rockies_df.distinct().count())
print(rockies_df.distinct().show(rockies_df.count()))
