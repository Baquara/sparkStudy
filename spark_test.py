# Import Spark libraries
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

# Create a SparkContext object
sc = SparkContext()

# Create an SQLContext object
sqlContext = SQLContext(sc)

# Load a text file and convert each line to a Row.
lines = sc.textFile("people.txt")
parts = lines.map(lambda l: l.split(","))
people = parts.map(lambda p: Row(name=p[0], age=int(p[1])))

# Infer the schema, and register the DataFrame as a table.
schemaPeople = sqlContext.createDataFrame(people)
schemaPeople.registerTempTable("people")

# SQL can be run over DataFrames that have been registered as a table.
teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

# The results of SQL queries are Dataframe objects.
# rdd returns the content as an :class:`pyspark.RDD` of :class:`Row`.
teenNames = teenagers.rdd.map(lambda p: "Name: " + p.name)
for teenName in teenNames.collect():
  print(teenName)

# We can also use functions instead of SQL queries:
schemaPeople.groupBy("age").count().show()

# The above code uses the Spark Core library to create a SparkContext, load and process data, and finally use Spark SQL to query the data.
# The code also uses the Spark SQL library to register the DataFrame as a table and query it using SQL.
# Finally, the code uses the Spark SQL library to group data by age and count it.
