from pyspark.sql import DataFrame
from pyspark.sql.functions import count, sum, avg, col, desc, window, hour
from pyspark.sql.window import Window
import sys
import os

# Add the project root to the path so we can import our modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.spark_session import create_spark_session
from scripts.read_data import read_cdr_data
from scripts.preprocess import preprocess_cdr_data

def analyze_user_activity(df: DataFrame):
    """
    Analyze user activity: calls per user, total duration, etc.
    
    Args:
        df (DataFrame): Preprocessed CDR data
        
    Returns:
        DataFrame: User activity summary
    """
    # Group by caller and calculate metrics
    user_activity = (df.groupBy("caller_number")
                    .agg(
                        count("*").alias("total_calls"),
                        sum("effective_duration").alias("total_duration_seconds"),
                        avg("effective_duration").alias("avg_duration_seconds"),
                        sum("call_success").alias("successful_calls"),
                        sum("cost").alias("total_cost"),
                        count(col("call_type") == "VOICE").alias("voice_calls"),
                        count(col("call_type") == "SMS").alias("sms_count"),
                        count(col("call_type") == "DATA").alias("data_sessions"),
                        sum("data_usage_kb").alias("total_data_usage_kb")
                    ))
    
    # Calculate call success rate
    user_activity = user_activity.withColumn(
        "call_success_rate", 
        col("successful_calls") / col("total_calls")
    )
    
    return user_activity

def identify_high_usage_users(user_activity: DataFrame, threshold_minutes=120):
    """
    Identify users with high usage (total duration > threshold)
    
    Args:
        user_activity (DataFrame): User activity summary
        threshold_minutes (int): Threshold in minutes for high usage
        
    Returns:
        DataFrame: High usage users
    """
    threshold_seconds = threshold_minutes * 60
    
    high_usage = user_activity.filter(
        col("total_duration_seconds") > threshold_seconds
    ).orderBy(desc("total_duration_seconds"))
    
    return high_usage

def analyze_call_patterns_by_time(df: DataFrame):
    """
    Analyze call patterns by hour of day
    
    Args:
        df (DataFrame): Preprocessed CDR data
        
    Returns:
        DataFrame: Call patterns by hour
    """
    # Group by hour and calculate metrics
    hourly_patterns = (df.groupBy("call_hour")
                      .agg(
                          count("*").alias("total_calls"),
                          sum("effective_duration").alias("total_duration_seconds"),
                          avg("effective_duration").alias("avg_duration_seconds"),
                          count(col("call_type") == "VOICE").alias("voice_calls"),
                          count(col("call_type") == "SMS").alias("sms_count"),
                          count(col("call_type") == "DATA").alias("data_sessions")
                      )
                      .orderBy("call_hour"))
    
    return hourly_patterns

def analyze_operator_performance(df: DataFrame):
    """
    Analyze operator performance
    
    Args:
        df (DataFrame): Preprocessed CDR data
        
    Returns:
        DataFrame: Operator performance metrics
    """
    # Group by operator and calculate metrics
    operator_perf = (df.groupBy("caller_operator")
                    .agg(
                        count("*").alias("total_calls"),
                        sum("effective_duration").alias("total_duration_seconds"),
                        sum("call_success").alias("successful_calls"),
                        count("*") - sum("call_success").alias("failed_calls")
                    ))
    
    # Calculate success rate
    operator_perf = operator_perf.withColumn(
        "success_rate", 
        col("successful_calls") / col("total_calls")
    )
    
    return operator_perf

if __name__ == "__main__":
    spark = create_spark_session("CDR Call Analysis")
    
    # Read and preprocess the data
    raw_df = read_cdr_data(spark)
    df = preprocess_cdr_data(raw_df)
    
    # Cache the data for better performance
    df.cache()
    
    # Analyze user activity
    user_activity = analyze_user_activity(df)
    print("User Activity Summary (Top 10):")
    user_activity.orderBy(desc("total_calls")).show(10)
    
    # Identify high usage users
    high_usage = identify_high_usage_users(user_activity)
    print("High Usage Users (Top 10):")
    high_usage.show(10)
    
    # Analyze call patterns by time
    hourly_patterns = analyze_call_patterns_by_time(df)
    print("Hourly Call Patterns:")
    hourly_patterns.show(24)
    
    # Analyze operator performance
    operator_perf = analyze_operator_performance(df)
    print("Operator Performance:")
    operator_perf.show()
    
    # Save results to CSV
    os.makedirs("results", exist_ok=True)
    
    user_activity.write.csv("results/user_activity.csv", header=True, mode="overwrite")
    high_usage.write.csv("results/high_usage_users.csv", header=True, mode="overwrite")
    hourly_patterns.write.csv("results/hourly_patterns.csv", header=True, mode="overwrite")
    operator_perf.write.csv("results/operator_performance.csv", header=True, mode="overwrite")
    
    spark.stop()