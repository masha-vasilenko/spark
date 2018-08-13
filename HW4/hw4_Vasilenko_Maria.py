from pyspark import SparkContext, SparkConf
from user_definition import *
#DO NOT ADD OTHER LIBRARIES/PACKAGES!

#Create SparkContext
conf = SparkConf().setMaster("local[*]").setAppName(app_name)
sc = SparkContext(conf=conf)

inputfile1 = sc.textFile(input_file1)
inputfile2 = sc.textFile(input_file2)
#1
sensor_ts = inputfile1.map(lambda x: x.split(',')).filter(lambda x: len(x)==5)
sensor_name_pair = inputfile2.map(lambda x: (int(x.split(',')[0]),x.split(',')[1]))
#Create pairs from input_file1
sensor_pairs = sensor_ts.map(lambda x: ((float(x[0]),int(x[1])),(float(x[2]),float(x[3]),float(x[4]))))

sum_count = sensor_pairs.combineByKey(lambda value: (value, 1),
                            lambda x, value: (map(sum,zip(x[0], value)), x[1] + 1),
                            lambda x, y: (map(sum,zip(x[0], y[0])), x[1] + y[1])
                           )

averageByKey = sum_count.map(lambda (key, (summ, count)): (key[1],[key[0], [summ[0]/ count, 
                                                                    summ[1]/count,
                                                                    summ[2]/count]])).sortBy(lambda x : x[0])
#2-3

joined = averageByKey.leftOuterJoin(sensor_name_pair).sortBy(lambda x: x[1][0])
joined_pair = joined.map(lambda x: ((x[0],x[1][1]),[x[1][0][0],x[1][0][1]]))
groupped = joined_pair.groupByKey().map(lambda x: (x[0],list(x[1]))).sortByKey().collect()

for elem in groupped:
    print elem[0][0],":",elem[0][1]
    
    for e in elem[1][:n_element]:
        print e
    print "..."
    for e in elem[1][len(elem[1])-n_element:]:
        print e
        

sc.stop()