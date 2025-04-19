from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# Initialize Spark
spark = SparkSession.Builder().appName("Simple PySpark Demo").getOrCreate()

# Define schema
schema = StructType([
    StructField("name", StringType(), False),
    StructField("age", IntegerType(), True),
    StructField("salary", FloatType(), True)
])

# Read sample CSV
df = spark.read.schema(schema).option("header", "true").csv("sample/data.csv")
df.show()
