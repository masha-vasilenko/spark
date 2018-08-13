from pyspark import SparkContext, SparkConf
from user_definition import *
import re, string

#Create SparkContext
conf = SparkConf().setMaster("local[*]").setAppName(app_name)
sc = SparkContext(conf=conf)

#Loading an external data
# lines = sc.textFile("input_3/input.txt")
lines = sc.textFile(input_file)

#Extract words from the text, write everything in lower case
def clean_text(text):
	text = re.sub("[^a-zA-Z ]"," ",text)
	text = [w.lower() for w in text.split()]
	text = sorted(text)
	return text


# Get distinct words with alphabetical characters in alphabetical order (lowercase)
words = lines.flatMap(clean_text)

words.collect()
distinct_words = words.distinct()
distinct_words = sorted(distinct_words.collect())

# Get the numbers from the text and filter those formatted appropriately

def get_num(text):
    comma = re.compile('(\d{1,3}(,\d{3})+)?(\.\d*)?')
    digit = re.compile('(0|\d*)?(\.\d*)?$')
    text = re.sub("[^0-9.,]", " ", text)
    split = text.split()
    res = []
    res1 = []

    for s in split:
    	s = string.rstrip(s, '.,')
        if s != '':
            res1.append(s)
            if comma.match(s) and comma.match(s).group() != '':
                s = comma.match(s).group()
                s = s.replace(",", "")
                res.append(float(s))
            elif digit.match(s) and digit.match(s).group() != '':
                s = digit.match(s).group()
                res.append(float(s))
    return res

nums = lines.flatMap(get_num)
numbers = nums.collect()

#Print words and sum of all numbers

for w in distinct_words:
    print w
print('')
print sum(numbers)

sc.stop()