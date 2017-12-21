from pyspark.sql import Row
from pyspark import SparkContext
from pyspark import SQLContext

sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
sqlContext = SQLContext(sc)

l = [('Ankit',25),('Jalfaizy',22),('saurabh',20),('Bala',26)]
rdd = sc.parallelize(l)
people = rdd.map(lambda x: Row(name=x[0], age=int(x[1])))
schemaPeople = sqlContext.createDataFrame(people)

schemaPeople.show()
