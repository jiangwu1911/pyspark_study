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

def print_select_1(df):
    selectedData = sqlContext.sql("SELECT InvoiceID, TotalCost FROM cost WHERE TotalCost>0")
    selectedData.show()

def print_summary(df, column):
    df.describe(column).show()

def print_distinct(df, column):
    df.select(column).distinct().show()

sc = SparkContext(appName="spark_sql_test")
sc.setLogLevel("WARN")

csv_in = "054155548015-aws-billing-csv-2016-03.csv"

sqlContext = SQLContext(sc)
spdf_in = sqlContext.read.format('com.databricks.spark.csv')\
.options(delimiter=",").options(header="true")\
.options(header='true').load(csv_in)

spdf_in.registerTempTable("cost")

#print_schema(spdf_in)
#print_column(spdf_in)
#print_all_data(spdf_in)
#print_count(spdf_in)
#print_select_1(spdf_in)

#print_summary(spdf_in, 'TotalCost')
print_distinct(spdf_in, "ProductName")
