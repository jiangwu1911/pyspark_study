import re
from operator import add
from pyspark import SparkContext

sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
sc.setLogLevel("WARN")

file_in = sc.textFile('wc.py')
print('number of lines in file: %s' % file_in.count())

chars = file_in.map(lambda s: len(s)).reduce(add)
print('number of characters in file: %s' % chars)

# word count
words = file_in.flatMap(lambda line: re.split('\W+', line.lower().strip()))
words = words.filter(lambda x: len(x) > 3)
words = words.map(lambda w: (w,1))
words = words.reduceByKey(add)
print words.take(20)

#% matplotlib inline
import matplotlib.pyplot as plt

def histogram(words):
    count = map(lambda x: x[1], words)
    word = map(lambda x: x[0], words)
    plt.barh(range(len(count)), count, color='gray')
    plt.yticks(range(len(count)), word)

words = words.map(lambda x:(x[1], x[0]))
words.take(20)
