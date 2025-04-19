import os
import sys
import time
from utils.spark_session import create_spark_session

def run_pipeline():
    """
    Run the complete CDR analysis pipeline
    """
    start_time = time.time()
    
    print("="*50)
    print("CDR Analysis Pipeline")
    print("="*50)
    
    # Step 1: Generate sample data
    print("\nStep 1: Generating sample data...")
    os.system("python scripts/data_generator.py")
    
    # Step 2: Read and preprocess data
    print("\nStep 2: Reading and preprocessing data...")
    os.system("python scripts/read_data.py")
    os.system("python scripts/preprocess.py")
    
    # Step 3: Analyze call data
    print("\nStep 3: Analyzing call data...")
    os.system("python scripts/analyze_calls.py")
    
    # Step 4: Analyze usage patterns
    print("\nStep 4: Analyzing usage patterns...")
    os.system("python scripts/usage_patterns.py")
    
    # Step 5: Generate visualizations
    print("\nStep 5: Generating visualizations...")
    os.system("python scripts/visualize_results.py")
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    print("\n" + "="*50)
    print(f"Pipeline completed in {elapsed_time:.2f} seconds")
    print("Results saved to 'results' directory")
    print("Visualizations saved to 'plots' directory")
    print("="*50)

if __name__ == "__main__":
    run_pipeline()