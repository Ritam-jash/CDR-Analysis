from pyspark.sql import DataFrame
from pyspark.sql.functions import col, hour, dayofweek, count, sum, avg, desc
import sys
import os

# Add the project root to the path so we can import our modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.spark_session import create_spark_session
from scripts.read_data import read_cdr_data
from scripts.preprocess import preprocess_cdr_data

def analyze_daily_patterns(df: DataFrame):
    """
    Analyze CDR patterns by day of week
    
    Args:
        df (DataFrame): Preprocessed CDR data
        
    Returns:
        DataFrame: Daily usage patterns
    """
    # Extract day of week (1 = Sunday, 7 = Saturday)
    df = df.withColumn("day_of_week", dayofweek(col("timestamp")))
    
    # Group by day of week
    daily_patterns = (df.groupBy("day_of_week")
                     .agg(
                         count("*").alias("total_calls"),
                         sum("effective_duration").alias("total_duration_seconds"),
                         avg("effective_duration").alias("avg_duration_seconds"),
                         count(col("call_type") == "VOICE").alias("voice_calls"),
                         count(col("call_type") == "SMS").alias("sms_count"),
                         count(col("call_type") == "DATA").alias("data_sessions"),
                         sum("data_usage_kb").alias("total_data_usage_kb"),
                         sum("cost").alias("total_cost")
                     )
                     .orderBy("day_of_week"))
    
    return daily_patterns

def analyze_hourly_patterns(df: DataFrame):
    """
    Analyze CDR patterns by hour of day for each call type
    
    Args:
        df (DataFrame): Preprocessed CDR data
        
    Returns:
        DataFrame: Hourly patterns by call type
    """
    # Group by hour and call type
    hourly_by_type = (df.groupBy("call_hour", "call_type")
                     .agg(
                         count("*").alias("total_calls"),
                         sum("effective_duration").alias("total_duration_seconds"),
                         avg("effective_duration").alias("avg_duration_seconds"),
                         sum("data_usage_kb").alias("total_data_usage_kb"),
                         sum("cost").alias("total_cost")
                     )
                     .orderBy("call_hour", "call_type"))
    
    return hourly_by_type

def identify_peak_hours(df: DataFrame):
    """
    Identify peak hours by call volume
    
    Args:
        df (DataFrame): Preprocessed CDR data
        
    Returns:
        DataFrame: Peak hours by call volume
    """
    # Group by hour and count calls
    hourly_volume = (df.groupBy("call_hour")
                    .count()
                    .withColumnRenamed("count", "call_volume")
                    .orderBy(desc("call_volume")))
    
    return hourly_volume

def analyze_call_durations(df: DataFrame):
    """
    Analyze call duration distribution
    
    Args:
        df (DataFrame): Preprocessed CDR data with only VOICE calls
        
    Returns:
        DataFrame: Duration distribution
    """
    # Filter for voice calls only
    voice_calls = df.filter(col("call_type") == "VOICE")
    
    # Create duration buckets
    voice_calls = voice_calls.withColumn(
        "duration_bucket"
        .when(col("duration") < 60, "< 1 min")
        .when(col("duration") < 300, "1-5 mins")
        .when(col("duration") < 600, "5-10 mins")
        .when(col("duration") < 1800, "10-30 mins")
        .otherwise("> 30 mins")
    )
    
    # Group by duration bucket
    duration_dist = (voice_calls.groupBy("duration_bucket")
                    .count()
                    .withColumnRenamed("count", "num_calls"))
    
    return duration_dist

if __name__ == "__main__":
    spark = create_spark_session("CDR Usage Patterns Analysis")
    
    # Read and preprocess the data
    raw_df = read_cdr_data(spark)
    df = preprocess_cdr_data(raw_df)
    
    # Cache the data for better performance
    df.cache()
    
    # Analyze daily patterns
    daily_patterns = analyze_daily_patterns(df)
    print("Daily Usage Patterns:")
    daily_patterns.show()
    
    # Analyze hourly patterns by call type
    hourly_by_type = analyze_hourly_patterns(df)
    print("Hourly Patterns by Call Type (Sample):")
    hourly_by_type.show(10)
    
    # Identify peak hours
    peak_hours = identify_peak_hours(df)
    print("Peak Hours by Call Volume:")
    peak_hours.show(5)
    
    # Create directory for results
    os.makedirs("results", exist_ok=True)
    
    # Save results
    daily_patterns.write.csv("results/daily_patterns.csv", header=True, mode="overwrite")
    hourly_by_type.write.csv("results/hourly_by_type.csv", header=True, mode="overwrite")
    peak_hours.write.csv("results/peak_hours.csv", header=True, mode="overwrite")
    
    spark.stop()