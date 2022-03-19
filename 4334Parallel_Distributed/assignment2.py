# Databricks notebook source
#Ethan Engel assignment2
from pyspark.sql.functions import col, explode
from pyspark.sql import functions as f
from pyspark.ml import Pipeline
from pyspark.sql.types import DoubleType,IntegerType, StructType, StructField
from pyspark.ml.feature import Bucketizer, Binarizer, StringIndexer,VectorAssembler
from pyspark.ml.regression import LinearRegression

personality_df=spark.read.format("csv").option("header", "true").option("inferSchema",True).load("dbfs:///FileStore/tables/assignment2/personality_traits.csv")
trimmed_personality_df=personality_df.drop("age","NBFI1","EBFI1","OBFI1","ABFI1","CBFI1","NBFI2","EBFI2","OBFI2","ABFI2","CBFI2","NBFI3","EBFI3","OBFI3","ABFI3","CBFI3","grandiosity","uniqueness","charmingness","devaluation","supremacy","agresiveness","DELTA20FINAL_1","DELTA20FINAL_2","DELTA20FINAL_3","DELTA20TOT","delta2itema1","delta2itema2","delta2itema3")
trimmed_personality_df=trimmed_personality_df.filter(trimmed_personality_df.age_new >= 21).withColumn("age_new",trimmed_personality_df.age_new.cast('int'))
trimmed_personality_df=trimmed_personality_df.withColumn("nar_sum", col("admiration")+col("rivalry")).drop("admiration","rivalry")


narc_train, narc_test = trimmed_personality_df.randomSplit(weights=[0.7,0.3], seed=200)
narc_test=narc_test.repartition(20)
dbutils.fs.rm("dbfs:///FileStore/tables/assignment2partitions/", True)
narc_test.write.format("csv").option("header",True).save("dbfs:///FileStore/tables/assignment2partitions/")

age_splits=[-float("inf"),20,30,40,50,60,float("inf")]
age_Bucketizer=Bucketizer(splits=age_splits,inputCol="age_new",outputCol="ageBucket")
ind1=StringIndexer(inputCol="gender", outputCol="gender_index")
vectorAssembler = VectorAssembler(inputCols = ['gender','age_new','NBFI','EBFI','OBFI','ABFI','CBFI'], outputCol = 'features')
linreg= LinearRegression(featuresCol = 'features', labelCol='nar_sum', maxIter=10, regParam=0.3, elasticNetParam=0.8)
p=Pipeline(stages=[age_Bucketizer,ind1,vectorAssembler,linreg])
linreg_model = p.fit(narc_train)

print(linreg_model.stages[3].coefficients)
print(linreg_model.stages[3].intercept)

test_schema=StructType( \
[StructField('gender', IntegerType(), True),
StructField('age_new', IntegerType(), True), \
StructField('NBFI',DoubleType(), True),\
StructField('EBFI',DoubleType(), True),\
StructField('OBFI',DoubleType(), True),\
StructField('ABFI',DoubleType(), True),\
StructField('CBFI',DoubleType(), True),\
StructField('nar_sum',DoubleType(), True)])


test_stream_df=spark.readStream.format("csv").option("header",True).schema(test_schema).load("dbfs:///FileStore/tables/assignment2partitions/")

test_preds=linreg_model.transform(test_stream_df).drop("features",'NBFI','EBFI','OBFI','ABFI','CBFI',"gender","age_new","ageBucket","gender_index")

sinkStream=test_preds.writeStream.outputMode("update").format("memory").queryName("tq").trigger(processingTime='1 minute').start()




# COMMAND ----------

#new COMMAND

preds_stream=spark.sql("SELECT * FROM tq")
from pyspark.ml.evaluation import RegressionEvaluator

preds_stream.show()
lr_evaluator = RegressionEvaluator(predictionCol="prediction", \
labelCol="nar_sum",metricName="rmse")
print("RMSE:", lr_evaluator.evaluate(preds_stream))
