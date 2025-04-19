from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import sys
import os

# Add the project root to the path so we can import our utils module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.spark_session import create_spark_session

def define_schema():
    """
    Define the schema for the CDR data
    
    Returns:
        StructType: Schema for CDR data
    """
    return StructType([
        StructField("caller_number", StringType(), False),
        StructField("callee_number", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("call_type", StringType(), False),
        StructField("duration", IntegerType(), False),
        StructField("status", StringType(), False),
        StructField("caller_operator", StringType(), False),
        StructField("callee_operator", StringType(), False),
        StructField("data_usage_kb", IntegerType(), True),
        StructField("cost", DoubleType(), True)
    ])

def read_cdr_data(spark, data_path="data/call_logs.csv") -> DataFrame:
    """
    Read CDR data from CSV file using the defined schema
    
    Args:
        spark: SparkSession object
        data_path (str): Path to the CSV file
        
    Returns:
        DataFrame: Spark DataFrame containing CDR data
    """
    schema = define_schema()
    
    # Read CSV with the defined schema
    df = (spark.read
          .format("csv")
          .option("header", "true")
          .option("mode", "DROPMALFORMED")
          .schema(schema)
          .load(data_path))
    
    print(f"Loaded {df.count()} records")
    return df

if __name__ == "__main__":
    spark = create_spark_session("CDR Data Loading")
    df = read_cdr_data(spark)
    df.show(5)
    df.printSchema()
    
    # Cache the DataFrame for faster access in subsequent operations
    df.cache()
    
    # Save as a temporary view for SQL queries
    df.createOrReplaceTempView("cdr_data")
    
    # Example SQL query
    print("Sample SQL query result:")
    spark.sql("SELECT call_type, COUNT(*) as count FROM cdr_data GROUP BY call_type").show()
    
    # Stop the SparkSession
    spark.stop()