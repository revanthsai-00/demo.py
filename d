from pyspark.sql import SparkSession
database = "Coviddata" #your database name
collection = "region" #your collection name
connectionString=   ('mongodb+srv://Revanth:cheeku@cluster0.l7yfihd.mongodb.net/?retryWrites=true&w=majority')
spark = SparkSession\
.builder\
    .config('spark.mongodb.input.uri',connectionString)\
    .config('spark.mongodb.output.uri', connectionString)\
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1')\
.getOrCreate()\
# Reading from MongoDB
region = spark.read\
.format("com.mongodb.spark.sql.DefaultSource")\
.option("uri", connectionString)\
.option("database", database)\
.option("collection", collection)\
.load()
region.show()
