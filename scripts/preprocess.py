# from pyspark.sql import DataFrame
# from pyspark.sql.functions import to_timestamp, when, col, isnull, sum as spark_sum
# import sys
# import os

# # Add the project root to the path so we can import our modules
# sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# from utils.spark_session import create_spark_session
# from scripts.read_data import read_cdr_data

# def preprocess_cdr_data(df: DataFrame) -> DataFrame:
#     """
#     Preprocess the CDR data:
#     1. Convert timestamp string to timestamp type
#     2. Handle null values
#     3. Add additional columns for analysis
    
#     Args:
#         df (DataFrame): Raw CDR data
        
#     Returns:
#         DataFrame: Preprocessed CDR data
#     """
#     # Convert timestamp string to timestamp type
#     df = df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
    
#     # Extract date and time components
#     df = df.withColumn("call_date", col("timestamp").cast("date"))
#     df = df.withColumn("call_hour", col("timestamp").cast("hour"))
    
#     # Handle null values in data_usage_kb
#     df = df.withColumn("data_usage_kb", 
#                       when(isnull(col("data_usage_kb")), 0)
#                       .otherwise(col("data_usage_kb")))
    
#     # Add a call_success column (1 for successful calls, 0 for failed)
#     df = df.withColumn("call_success", 
#                       when(col("status") == "ANSWERED", 1)
#                       .otherwise(0))
    
#     # Add effective duration (0 for failed calls)
#     df = df.withColumn("effective_duration", 
#                       when(col("status") == "ANSWERED", col("duration"))
#                       .otherwise(0))
    
#     # Check if there are any invalid durations (negative)
#     invalid_durations = df.filter(col("duration") < 0).count()
#     if invalid_durations > 0:
#         print(f"Warning: Found {invalid_durations} records with negative duration.")
#         df = df.filter(col("duration") >= 0)
    
#     return df

# def validate_data_quality(df: DataFrame) -> None:
#     """
#     Validate the quality of the data and print statistics
    
#     Args:
#         df (DataFrame): CDR data
#     """
#     total_records = df.count()
#     null_values = df.select([spark_sum(isnull(c).cast("int")).alias(c) for c in df.columns])
    
#     print("Data Quality Report:")
#     print(f"Total Records: {total_records}")
#     print("Null Values:")
#     null_values.show()
    
#     # Check for invalid call types
#     valid_call_types = ['VOICE', 'SMS', 'DATA']
#     invalid_call_types = df.filter(~col("call_type").isin(valid_call_types)).count()
#     print(f"Invalid Call Types: {invalid_call_types}")

# if __name__ == "__main__":
#     spark = create_spark_session("CDR Data Preprocessing")
    
#     # Read the data
#     raw_df = read_cdr_data(spark)
    
#     # Preprocess the data
#     processed_df = preprocess_cdr_data(raw_df)
    
#     # Validate data quality
#     validate_data_quality(processed_df)
    
#     # Show sample of processed data
#     print("\nSample of processed data:")
#     processed_df.show(5)
    
#     # Cache the processed data
#     processed_df.cache()
#     processed_df.createOrReplaceTempView("processed_cdr_data")
    
#     # Save the processed data for next steps
#     # processed_df.write.parquet("data/processed_cdr_data.parquet", mode="overwrite")
    
#     spark.stop()












# from pyspark.sql import DataFrame
# from pyspark.sql.functions import to_timestamp, when, col, isnull, sum as spark_sum, hour
# import sys
# import os

# # Add the project root to the path so we can import our modules
# sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# from utils.spark_session import create_spark_session
# from scripts.read_data import read_cdr_data

# def preprocess_cdr_data(df: DataFrame) -> DataFrame:
#     """
#     Preprocess the CDR data:
#     1. Convert timestamp string to timestamp type
#     2. Handle null values
#     3. Add additional columns for analysis
    
#     Args:
#         df (DataFrame): Raw CDR data
        
#     Returns:
#         DataFrame: Preprocessed CDR data
#     """
#     # Convert timestamp string to timestamp type
#     df = df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
    
#     # Extract date and time components
#     df = df.withColumn("call_date", col("timestamp").cast("date"))
#     df = df.withColumn("call_hour", hour(col("timestamp")))  # Corrected line
    
#     # Handle null values in data_usage_kb
#     df = df.withColumn("data_usage_kb", 
#                       when(isnull(col("data_usage_kb")), 0)
#                       .otherwise(col("data_usage_kb")))
    
#     # Add a call_success column (1 for successful calls, 0 for failed)
#     df = df.withColumn("call_success", 
#                       when(col("status") == "ANSWERED", 1)
#                       .otherwise(0))
    
#     # Add effective duration (0 for failed calls)
#     df = df.withColumn("effective_duration", 
#                       when(col("status") == "ANSWERED", col("duration"))
#                       .otherwise(0))
    
#     # Check if there are any invalid durations (negative)
#     invalid_durations = df.filter(col("duration") < 0).count()
#     if invalid_durations > 0:
#         print(f"Warning: Found {invalid_durations} records with negative duration.")
#         df = df.filter(col("duration") >= 0)
    
#     return df

# def validate_data_quality(df: DataFrame) -> None:
#     """
#     Validate the quality of the data and print statistics
    
#     Args:
#         df (DataFrame): CDR data
#     """
#     total_records = df.count()
#     null_values = df.select([spark_sum(isnull(c).cast("int")).alias(c) for c in df.columns])
    
#     print("Data Quality Report:")
#     print(f"Total Records: {total_records}")
#     print("Null Values:")
#     null_values.show()
    
#     # Check for invalid call types
#     valid_call_types = ['VOICE', 'SMS', 'DATA']
#     invalid_call_types = df.filter(~col("call_type").isin(valid_call_types)).count()
#     print(f"Invalid Call Types: {invalid_call_types}")

# if __name__ == "__main__":
#     spark = create_spark_session("CDR Data Preprocessing")
    
#     # Read the data
#     raw_df = read_cdr_data(spark)
    
#     # Preprocess the data
#     processed_df = preprocess_cdr_data(raw_df)
    
#     # Validate data quality
#     validate_data_quality(processed_df)
    
#     # Show sample of processed data
#     print("\nSample of processed data:")
#     processed_df.show(5)
    
#     # Cache the processed data
#     processed_df.cache()
#     processed_df.createOrReplaceTempView("processed_cdr_data")
    
#     # Save the processed data for next steps
#     # processed_df.write.parquet("data/processed_cdr_data.parquet", mode="overwrite")
    
#     spark.stop()

































from pyspark.sql import DataFrame
from pyspark.sql.functions import to_timestamp, when, col, isnull, sum as spark_sum, hour
import sys
import os

# Add the project root to the path so we can import our modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.spark_session import create_spark_session
from scripts.read_data import read_cdr_data

def preprocess_cdr_data(df: DataFrame) -> DataFrame:
    """
    Preprocess the CDR data:
    1. Convert timestamp string to timestamp type
    2. Handle null values
    3. Add additional columns for analysis
    
    Args:
        df (DataFrame): Raw CDR data
        
    Returns:
        DataFrame: Preprocessed CDR data
    """
    # Convert timestamp string to timestamp type
    df = df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
    
    # Extract date and time components
    df = df.withColumn("call_date", col("timestamp").cast("date"))
    df = df.withColumn("call_hour", hour(col("timestamp")))  # Corrected line
    
    # Handle null values in data_usage_kb
    df = df.withColumn("data_usage_kb", 
                      when(isnull(col("data_usage_kb")), 0)
                      .otherwise(col("data_usage_kb")))
    
    # Add a call_success column (1 for successful calls, 0 for failed)
    df = df.withColumn("call_success", 
                      when(col("status") == "ANSWERED", 1)
                      .otherwise(0))
    
    # Add effective duration (0 for failed calls)
    df = df.withColumn("effective_duration", 
                      when(col("status") == "ANSWERED", col("duration"))
                      .otherwise(0))
    
    # Check if there are any invalid durations (negative)
    invalid_durations = df.filter(col("duration") < 0).count()
    if invalid_durations > 0:
        print(f"Warning: Found {invalid_durations} records with negative duration.")
        df = df.filter(col("duration") >= 0)
    
    return df

def validate_data_quality(df: DataFrame) -> None:
    """
    Validate the quality of the data and print statistics
    
    Args:
        df (DataFrame): CDR data
    """
    total_records = df.count()
    null_values = df.select([spark_sum(isnull(c).cast("int")).alias(c) for c in df.columns])
    
    print("Data Quality Report:")
    print(f"Total Records: {total_records}")
    print("Null Values:")
    null_values.show()
    
    # Check for invalid call types
    valid_call_types = ['VOICE', 'SMS', 'DATA']
    invalid_call_types = df.filter(~col("call_type").isin(valid_call_types)).count()
    print(f"Invalid Call Types: {invalid_call_types}")

if __name__ == "__main__":
    spark = create_spark_session("CDR Data Preprocessing")
    
    # Read the data
    raw_df = read_cdr_data(spark)
    
    # Preprocess the data
    processed_df = preprocess_cdr_data(raw_df)
    
    # Validate data quality
    validate_data_quality(processed_df)
    
    # Show sample of processed data
    print("\nSample of processed data:")
    processed_df.show(5)
    
    # Cache the processed data
    processed_df.cache()
    processed_df.createOrReplaceTempView("processed_cdr_data")
    
    # Save the processed data for next steps
    # processed_df.write.parquet("data/processed_cdr_data.parquet", mode="overwrite")
    
    spark.stop()