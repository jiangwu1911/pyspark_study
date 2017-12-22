from pyspark import SparkContext
from pyspark import SQLContext

################################
# Sample: https://www.analyticsvidhya.com/blog/2016/10/spark-dataframe-and-operations/
################################

def print_schema(df):
    spdf_in.printSchema() 

def print_column(df):
    print(df.columns)

def print_all_data(df):
    df.show()

def print_count(df):
    print(df.count())

def print_head(df):
    print(df.head(5))

def print_summary(df, column):
    df.describe(column).show()

def print_distinct(df, column):
    df.select(column).distinct().show()

def select_column(df):
    df.select('User_ID', 'Age').show(5)

def print_crosstab(df):
    df.crosstab('Age', 'Gender').show()

def print_filter(df):
    print(df.filter(df.Purchase > 15000).count())

def print_mean(df):
    df.groupby('Age').agg({'Purchase': 'mean'}).show()
    df.groupby('Age').count().show()

def print_sample(df):
    t1 = df.sample(False, 0.2, 42)
    print(t1.count())

def print_orderby(df):
    df.orderBy(df.Purchase.desc()).show(5)

def print_new_column(df):
    df.withColumn('Purchase_new', df.Purchase/2.0).select('Purchase', 'Purchase_new').show(5)

def drop_column(df):
    print(df.drop('Comb').columns)

def print_sql_select(df, sqlContext):
    #sqlContext.sql('SELECT Product_ID FROM train_table').show(5)
    sqlContext.sql('SELECT Age, max(Purchase) FROM train_table GROUP BY Age').show(5)


sc = SparkContext(appName="spark_black_friday")
sc.setLogLevel("WARN")

csv_in = "data/train.csv"

sqlContext = SQLContext(sc)
spdf_in = sqlContext.read.format('com.databricks.spark.csv')\
.options(delimiter=",").options(header="true")\
.options(header='true').load(csv_in)

spdf_in.registerTempTable("train_table")

#print_schema(spdf_in)
#print_column(spdf_in)
#print_all_data(spdf_in)
#print_count(spdf_in)
#print_head(spdf_in)

#print_summary(spdf_in, 'Purchase')
#select_column(spdf_in)
#print_distinct(spdf_in, 'Product_ID')
#print_crosstab(spdf_in)
#print_filter(spdf_in)

#print_mean(spdf_in)
#print_sample(spdf_in)
#print_orderby(spdf_in)
#print_new_column(spdf_in)
#drop_column(spdf_in)

print_sql_select(spdf_in, sqlContext)
