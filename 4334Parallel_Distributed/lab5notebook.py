sc = spark.sparkContext
baseball_rdd5 = sc.textFile("dbfs:///FileStore/tables/lab5/baseball_master.csv")
header = baseball_rdd5.first()
baseball_rdd6 = baseball_rdd5.filter(lambda row: row != header)

def splitfunc(x):
    list=x.split(',')
    return [list[0],list[4],list[5],list[17]]
trimmed_rdd=baseball_rdd6.map(splitfunc)
trimmed_rdd2=trimmed_rdd.filter(lambda y: ""  not in y)
#print(trimmed_rdd2.take(25))

#from pyspark.sql.functions import  col, desc
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType,LongType
baseballSchema=StructType([StructField('playerID',StringType(),True),StructField('birthCountry',StringType(),True),StructField('birthState',StringType(),True),StructField('Height',LongType(),False)])


baseball_rows = trimmed_rdd2.map(lambda x: Row(playerID=x[0],birthCountry=x[1],birthState=x[2],Height=int(x[3])))
baseball_df=spark.createDataFrame(baseball_rows, baseballSchema)
baseball_df.createOrReplaceTempView('baseball_table')

spark.sql("""SELECT COUNT(birthState) FROM baseball_table WHERE birthState='CO'""").show()
print(baseball_df.filter("birthState='CO'").count())

baseball_df.groupBy("birthCountry").agg(avg("Height").alias("Avg_ht")).sort(col("Avg_ht").desc()).show()
spark.sql("""SELECT birthCountry, avg(Height) AS avg_ht FROM baseball_table GROUP BY birthCountry ORDER BY avg_ht DESC""").show()
