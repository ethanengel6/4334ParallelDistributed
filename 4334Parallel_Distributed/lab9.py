from graphframes import *
from pyspark.sql.functions import explode

route_edges_df=spark.read.format("csv").option("header","true").option('mode','dropMalformed')\
.option("inferSchema","true").load("dbfs:///FileStore/tables/lab9/routes.csv")

airport_verts_df=spark.read.format("csv").option('mode','dropMalformed')\
.option("inferSchema","true").load("dbfs:///FileStore/tables/lab9/airports.csv")

cleaned_verts_df=airport_verts_df.filter(airport_verts_df._c3 == "United States").drop("_c0","_c1","_c2","_c3","_c5","_c6","_c7",\
"_c8","_c9","_c10","_c11","_c12","_c13").dropDuplicates().withColumnRenamed("_c4","id")

distinct_ids = cleaned_verts_df.select("id").distinct()
usa_list =distinct_ids.select('id').rdd.flatMap(lambda x: x).collect()
#print(usa_list)

trimmededges_df=route_edges_df.drop("airline","airline ID"," source airport id"," destination airport id"," codeshare"," stops"," equipment").\
withColumnRenamed(" source airport","src").withColumnRenamed(" destination apirport","dst").dropDuplicates()

usa1_df=trimmededges_df.filter(trimmededges_df.src.isin(usa_list))
usa2_df=usa1_df.filter(usa1_df.dst.isin(usa_list))


graph=GraphFrame(cleaned_verts_df,usa2_df)
print("Number of US airports:",graph.vertices.count())
print("Number of US to US routes",graph.edges.count())

denver_df=graph.find("(a)-[e]->(b);(b)-[e2]->(c)").filter("a.id='DEN'" or "c.id='DEN'")
airports = denver_df.select("b")

denver_round_df=graph.find("(a)-[e]->(b);(b)-[e2]->(a)").filter("a.id='DEN'" or "c.id='DEN'")
round_airports=denver_round_df.select("b")

oneway_ports=airports.subtract(round_airports).withColumnRenamed("b","IATA")
oneway_ports.show()

paths = graph.shortestPaths(landmarks=["DEN"])
expl_paths=paths.select("id", explode("distances")).filter("value>3").withColumnRenamed("id","IATA").withColumnRenamed("value","hops")
expl_paths.select("IATA","hops").show()
