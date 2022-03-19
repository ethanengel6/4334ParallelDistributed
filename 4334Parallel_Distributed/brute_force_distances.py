import csv

file = open("brute_force_pts.csv", "r")
csv_reader = csv.reader(file)

pts_list = []
for row in csv_reader:
    pts_list.append(row)
    row[1]=round(float(row[1]),3)
    row[2]=round(float(row[2]),3)


close_list=[]
for pt in range(len(pts_list)-1):
    z=pt+1
    while z<len(pts_list):

        dist=(((pts_list[pt][1]-pts_list[z][1])**2+(pts_list[pt][2]-pts_list[z][2])**2)**.5)
        if dist<.75:
            close_list.append((pts_list[pt][0],pts_list[z][0], dist))
        z+=1
print(close_list)
