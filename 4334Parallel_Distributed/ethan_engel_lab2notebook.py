# Databricks notebook source
#Ethan Engel
sc=spark.sparkContext
rlist=range(100,10000)
rdd1=sc.parallelize(rlist)
def isPrime(p):
    for q in range(2, int(p**.5+1)):
        if p%q==0:
            return False
    return True
prime_rdd=rdd1.filter(isPrime)
print(prime_rdd.count())

# COMMAND ----------

sc=spark.sparkContext
import random
randomlist = []
for i in range(0,1000):
    n = random.randint(0,100)
    randomlist.append(n)
rand_rdd=sc.parallelize(randomlist)
cels_rdd=rand_rdd.map(lambda x:(x-32)/1.8).persist()
print(cels_rdd.reduce(lambda x,y:x+y)/cels_rdd.count())

