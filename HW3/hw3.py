from pyspark import SparkContext, SparkConf
from user_definition import *
import sys
#DO NOT ADD ADDITIONAL LIBRARIES!!

Create SparkContext
conf = SparkConf().setMaster("local[*]").setAppName(app_name)
sc = SparkContext(conf=conf)

#Loading an external data
# lines = sc.textFile("input_3/input.txt")

def check_zip(line):
	'''check if zip code is non-empty and has 5 digits'''
    res = True
    zip_code = line.split(',')[0]
    if len(zip_code) != 5:
        res = False
        return res
    for c in zip_code:
        if c not in ['0','1','2','3','4','5','6','7','8','9']:
            res = False
            return res
    return res

 def clean(elem):
 	'''sort the list and replace None values with "NULL"'''
    l = sorted(elem)
    res = []
    for s in l:
        if s == '':
            s = s.replace('',"NULL")
        res.append(s)
    return res

#1
text = sc.textFile(input_file)
# Remove duplicates
text_distinct = text.distinct()
#  Filter out lines with improper number of columns
text_full = text_distinct.filter(lambda x:len(x.split(','))==5)
# Filter lines with 5-digit zip codes
text_zip = text_full.filter(check_zip)

#2 
#Create key-value pairs: (zip,1)
zip_pair = text_zip.map(lambda x: (x.split(',')[0],1))
# Count the number of businesses (lines)
zip_count = zip_pair.reduceByKey(lambda x,y: x+y)
#Sort by values (number of businesses)
zip_count_sorted = zip_count.map(lambda x: (x[1],x[0])).sortByKey(False).map(lambda x: (x[1],x[0])) # sortByKey(False) returns descendng order

for z in zip_count_sorted.collect()[:10]:
    print '%s : %d' % (z[0],z[1])

print " "

# 3
# Create key-value pairs: (zip, state), group by key and sort by key
zip_state = text_zip.map(lambda x: (x.split(',')[0],x.split(',')[4]))
zip_state_group = zip_state.groupByKey().map(lambda x: (x[0],list(x[1]))).sortByKey(False)
# Create unique set of values (states)
zip_state_unique = zip_state_group.mapValues(lambda x: set(x))
# Leave only pairs with more than one state per zip code
zip_state_many = zip_state_unique.filter(lambda x: len(x[1])>1)
# Sort and clean the list of zip codes and print them

for z in zip_state_many.mapValues(clean).collect():
    print z[0],":", ', '.join(z[1])



