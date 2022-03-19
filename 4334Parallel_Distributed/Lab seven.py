#Ethan Engel

# Databricks notebook source
dbutils.fs.mkdirs("FileStore/tables/lab7")

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.feature import Bucketizer, Binarizer, StringIndexer,VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import col
from pyspark.ml.evaluation import BinaryClassificationEvaluator


train_df=spark.read.format("csv").option("header","true").option("inferSchema","true").load("dbfs:///FileStore/tables/lab7/heartTraining.csv")
test_df=spark.read.format("csv").option("header","true").option("inferSchema","true").load("dbfs:///FileStore/tables/lab7/heartTesting.csv")

age_splits=[-float("inf"),40,50,60,70,float("inf")]
age_Bucketizer=Bucketizer(splits=age_splits,inputCol="age",outputCol="ageBucket")

ind1=StringIndexer(inputCol="sex", outputCol="sexindex")
ind2=StringIndexer(inputCol="pred ",outputCol="predindex")
vectorAssembler = VectorAssembler(inputCols = ['sexindex','age', 'chol'], outputCol = 'features')
lr=LogisticRegression(maxIter=10,regParam=.01,featuresCol = 'features', labelCol='predindex')


p=Pipeline(stages=[age_Bucketizer,ind1,ind2,vectorAssembler,lr])
lrmodel=p.fit(train_df)

train_preds=lrmodel.transform(train_df)
predictions=lrmodel.transform(test_df)

trimmed_tr_preds=train_preds.drop("age","sex","chol","ageBucket","sexindex")
evaluator=BinaryClassificationEvaluator(rawPredictionCol='rawPrediction', labelCol='predindex')
print("Portion correctly predicted:",evaluator.evaluate(train_preds))
trimmed_predictions=predictions.drop("age","sex","chol","ageBucket","sexindex")
trimmed_tr_preds=train_preds.drop("age","sex","chol","ageBucket","sexindex")
trimmed_predictions.show(20)
