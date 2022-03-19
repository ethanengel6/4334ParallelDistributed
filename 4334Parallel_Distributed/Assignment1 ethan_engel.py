# Databricks notebook source
#Ethan Engel

pts_rdd = sc.textFile("dbfs:///FileStore/tables/assignment1")
#print(pts_rdd.collect())
def splitfunc(x):
    list=x.split(',')
    return list[0],float(list[1]),float(list[2])
split_rdd=pts_rdd.map(splitfunc)
#print(split_rdd.collect(),"\n\n")
def grid_plots(pt):
    coords=[pt[1],pt[2]]
    grid1=int(coords[0]//.75),int(coords[1]//.75)
    return (pt[0],pt[1],pt[2],(grid1[0],grid1[1])),((grid1[0],grid1[1]-1),(grid1[0]-1,grid1[1]),(grid1[0]-1,grid1[1]-1),(grid1[0],grid1[1]),(grid1[0],grid1[1]+1),\
    (grid1[0]+1,grid1[1]),(grid1[0]+1,grid1[1]+1),(grid1[0]+1,grid1[1]-1))


grid_assign_rdd=split_rdd.map(grid_plots)
#print(grid_assign_rdd.take(4),"\n\n")

grid_assign_split= grid_assign_rdd.flatMapValues(lambda q: q).sortByKey()
#print(grid_assign_split.take(4),"\n\n")

gridSwitched = grid_assign_split.map(lambda x: (x[1], x[0])).sortByKey()
#print(gridSwitched.take(10),"\n\n")

group2rdd = gridSwitched.groupByKey().map(lambda y: (y[0],list(y[1]))).sortByKey()
#print(group2rdd.take(15))


def distances(dist):
    close_list=[]
    if len(dist[1])<2:
        return (None)
    else:
        for pt in range(len(dist[1])-1):
            z=pt+1
            if ((dist[1][pt][3]!=dist[0]) and (dist[1][z][3]!=dist[0])) or ((dist[1][pt][3]!=dist[0]) and (dist[1][z][3]==dist[0])):
                z+=1
            else:
                while z<len(dist[1]):
                    distance=(((dist[1][pt][1]-dist[1][z][1])**2+(dist[1][pt][2]-dist[1][z][2])**2)**.5)
                    print(distance)
                    if distance<.75:
                        close_list.append((dist[1][pt][0],dist[1][z][0]))
                    z+=1
    return(close_list)

closepts_rdd=group2rdd.map(distances)

def trim(t):
    if t==None:
        return False
    elif t==[]:
        return False
    else:
        return True

trimmed_rdd=closepts_rdd.filter(trim)
trimmedflat=trimmed_rdd.flatMap(lambda x: x)
print(trimmedflat.count(),trimmedflat.collect())




# COMMAND ----------


