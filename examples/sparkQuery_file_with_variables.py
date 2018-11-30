from pyspark.sql import SparkSession

### Build spark session
spark = SparkSession.builder.getOrCreate()

### create DataFrames from CSV samples
customers=spark.read.csv("../datasets/input/customers.csv", header = True)
customers.registerTempTable("custDF")
orders=spark.read.csv("../datasets/input/orders.csv", header = True)
orders.registerTempTable("ordDF")

### Running spark-SQL already stored in separate location and pass parameters
output = spark.sql(open('sparkQueryFile.sql').read().format('custDF', 'ordDF',1,3))
output.write.csv('../datasets/output/sqlfile_results.csv')
