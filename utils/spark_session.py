from pyspark.sql import SparkSession

def create_spark_session(app_name="CDR Analysis"):
    """
    Create and return a SparkSession
    
    Args:
        app_name (str): Name of the Spark application
        
    Returns:
        SparkSession: Configured SparkSession object
    """
    return (SparkSession.builder
            .appName(app_name)
            .config("spark.executor.memory", "2g")
            .config("spark.driver.memory", "2g")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.ui.showConsoleProgress", "true")
            .getOrCreate())