from pyspark.sql import SparkSession

### Build spark session
spark = SparkSession.builder.getOrCreate()

### create DataFrames from CSV samples
customers=spark.read.csv("../datasets/input/customers.csv", header = True)
customers.registerTempTable("custDF")
orders=spark.read.csv("../datasets/input/orders.csv", header = True)
orders.registerTempTable("ordDF")

### Running spark-SQL on DFs 
output = spark.sql("select  a.id ,   a.name,b.balance from custDF a join ordDF b on a.id = b.id ")
output.write.csv('../datasets/output/joined_results.csv')
