sc = spark.sparkContext
def splitter(str):
    return str.split()
test_rdd=sc.parallelize(["a b c", "b a a", "c b", "d a"])
pairs = test_rdd.map(lambda x: (x.split(" ",1)[0], x.split(" ",1)[1]))
splitPairs= pairs.flatMapValues(splitter).sortByKey()
trimmed_pairs=splitPairs.map(lambda x:x).distinct()
links=trimmed_pairs.groupByKey().map(lambda x : (x[0], list(x[1]))).sortByKey()
print("Initial links:",links.collect())
rankings=sc.parallelize([('a', 0.25), ('b', 0.25), ('c', 0.25), ('d', 0.25)])
print("Initial rankings:",rankings.collect())
for ranks in range(10):
    print("Iteration:",ranks)
    temp=links.join(rankings)
    print("Joined RDD:",temp.collect())
    temp_mapped=temp.mapValues(lambda x: (x[0],(x[1]/len(x[0]))))
    temp_mapped_values=temp_mapped.values()
    rddSwitched = temp_mapped_values.map(lambda x: (x[1], x[0]))
    rddSwitchedSplit=rddSwitched.flatMap(lambda l: [(l[0], value) for value in l[1]])
    link_ratio = rddSwitchedSplit.map(lambda x: (x[1], x[0]))
    print("Neighbor contributions",link_ratio.collect())
    rankings=link_ratio.reduceByKey(lambda x,y:x+y)
    print("New rankings:",rankings.collect())
sortedrankings=rankings.sortBy(lambda x: x[1], ascending=0)
sortedlist=sortedrankings.collect()
for sr in sortedlist:
    print(sr[0],"has rank:",sr[1])


sc = spark.sparkContext

url1 = sc.textFile("dbfs:///FileStore/tables/shortLab3data1-3.txt")
url2 = sc.textFile("dbfs:///FileStore/tables/shortLab3data0-2.txt")
url3=url1+url2

def splitter(str):
    return str.split()

pairs2 = url3.map(lambda x: (x.split(" ",1)[0], x.split(" ",1)[1]))
splitPairs2= pairs2.flatMapValues(splitter).sortByKey()
trimmed_pairs2=splitPairs2.map(lambda x:x).distinct()
links2=trimmed_pairs2.groupByKey().map(lambda x : (x[0], list(x[1]))).sortByKey()
url_ranks=[]
for urls in range(20):
    url_ranks.append(("www.example"+str(urls+1)+".com",.05))
url_rdd=sc.parallelize(url_ranks)

for ranks in range(10):
    temp2=links2.join(url_rdd)
    #print("Joined RDD:",temp.collect())
    temp_mapped2=temp2.mapValues(lambda x: (x[0],(x[1]/len(x[0]))))
    temp_mapped_values2=temp_mapped2.values()
    rddSwitched2 = temp_mapped_values2.map(lambda x: (x[1], x[0]))
    rddSwitchedSplit2=rddSwitched2.flatMap(lambda l: [(l[0], value) for value in l[1]])
    link_ratio2 = rddSwitchedSplit2.map(lambda x: (x[1], x[0]))
    #print("Neighbor contributions",link_ratio.collect())
    url_rdd=link_ratio2.reduceByKey(lambda x,y:x+y)
sortedrankings2=url_rdd.sortBy(lambda x: x[1], ascending=0)
sortedlist2=sortedrankings2.collect()
print("\n")
for sr2 in sortedlist2:
    print(sr2[0],"has rank:",sr2[1])
