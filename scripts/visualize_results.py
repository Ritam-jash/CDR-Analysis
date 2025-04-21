

import pandas as pd
import matplotlib.pyplot as plt
import os
import glob
import sys

def load_csv_results(file_path):
    """
    Load a CSV file (or Spark CSV output directory) into a Pandas DataFrame.

    Args:
        file_path (str): Path to the CSV file or Spark output directory.
    Returns:
        DataFrame: Loaded DataFrame, or None if an error occurs.
    """
    import glob
    import os
    try:
        # If file_path is a directory (Spark output), look for part-*.csv
        if os.path.isdir(file_path):
            part_files = glob.glob(os.path.join(file_path, "part-*.csv"))
            if not part_files:
                print(f"No part files found in {file_path}")
                return None
            file_path = part_files[0]
        df = pd.read_csv(file_path)
        return df
    except Exception as e:
        print(f"Error loading CSV file: {file_path}")
        print(e)
        return None


def load_csv_results_old(file_path):
    """
    Load CSV results into a pandas DataFrame
    
    Args:
        file_path (str): Path to CSV file
        
    Returns:
        DataFrame: Pandas DataFrame
    """
    # Find the part file
    part_files = glob.glob(os.path.join(file_path, "part-*.csv"))
    if not part_files:
        print(f"No part files found in {file_path}")
        return None
    
    # Load the first part file (usually there's only one for small datasets)
    df = pd.read_csv(part_files[0], header=None)
    
    # Try to get header file
    header_file = os.path.join(file_path, "_SUCCESS")
    if os.path.exists(header_file):
        with open(header_file, 'r') as f:
            header = f.readline().strip().split(',')
        df.columns = header
    
    return df

def plot_hourly_patterns(results_dir="results"):
    """
    Plot hourly call patterns
    
    Args:
        results_dir (str): Directory containing results
    """
    # Load hourly patterns data
    hourly_file = os.path.join(results_dir, "hourly_patterns.csv")
    if not os.path.exists(hourly_file):
        print(f"Hourly patterns file not found: {hourly_file}")
        return
    
    df = load_csv_results(hourly_file)
    if df is None:
        return
    
    # Ensure we have the expected columns
    if 'call_hour' not in df.columns:
        df.columns = ['call_hour', 'total_calls', 'total_duration_seconds', 
                     'avg_duration_seconds', 'voice_calls', 'sms_count', 
                     'data_sessions']
    
    # Sort by hour
    df = df.sort_values('call_hour')
    
    # Create directory for plots
    os.makedirs("plots", exist_ok=True)
    
    # Plot total calls by hour
    plt.figure(figsize=(12, 6))
    plt.bar(df['call_hour'], df['total_calls'], color='blue')
    plt.xlabel('Hour of Day')
    plt.ylabel('Total Calls')
    plt.title('Total Calls by Hour of Day')
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.xticks(range(0, 24))
    plt.savefig("plots/total_calls_by_hour.png")
    plt.close()
    
    # Plot call types by hour
    plt.figure(figsize=(12, 6))
    plt.bar(df['call_hour'], df['voice_calls'], color='blue', label='Voice')
    plt.bar(df['call_hour'], df['sms_count'], bottom=df['voice_calls'], 
            color='green', label='SMS')
    plt.bar(df['call_hour'], df['data_sessions'], 
            bottom=df['voice_calls'] + df['sms_count'], 
            color='orange', label='Data')
    plt.xlabel('Hour of Day')
    plt.ylabel('Number of Calls')
    plt.title('Call Types by Hour of Day')
    plt.legend()
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.xticks(range(0, 24))
    plt.savefig("plots/call_types_by_hour.png")
    plt.close()

def plot_daily_patterns(results_dir="results"):
    """
    Plot daily call patterns
    
    Args:
        results_dir (str): Directory containing results
    """
    # Load daily patterns data
    daily_file = os.path.join(results_dir, "daily_patterns.csv")
    if not os.path.exists(daily_file):
        print(f"Daily patterns file not found: {daily_file}")
        return
    
    df = load_csv_results(daily_file)
    if df is None:
        return
    
    # Ensure we have the expected columns
    if 'day_of_week' not in df.columns:
        df.columns = ['day_of_week', 'total_calls', 'total_duration_seconds', 
                     'avg_duration_seconds', 'voice_calls', 'sms_count', 
                     'data_sessions', 'total_data_usage_kb', 'total_cost']
    
    # Sort by day of week
    df = df.sort_values('day_of_week')
    
    # Map day numbers to names
    days = {1: 'Sun', 2: 'Mon', 3: 'Tue', 4: 'Wed', 5: 'Thu', 6: 'Fri', 7: 'Sat'}
    df['day_name'] = df['day_of_week'].map(days)
    
    # Create directory for plots
    os.makedirs("plots", exist_ok=True)
    
    # Plot total calls by day
    plt.figure(figsize=(10, 6))
    plt.bar(df['day_name'], df['total_calls'], color='blue')
    plt.xlabel('Day of Week')
    plt.ylabel('Total Calls')
    plt.title('Total Calls by Day of Week')
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.savefig("plots/total_calls_by_day.png")
    plt.close()
    
    # Plot average duration by day
    plt.figure(figsize=(10, 6))
    plt.bar(df['day_name'], df['avg_duration_seconds'] / 60, color='green')
    plt.xlabel('Day of Week')
    plt.ylabel('Average Duration (minutes)')
    plt.title('Average Call Duration by Day of Week')
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.savefig("plots/avg_duration_by_day.png")
    plt.close()

def plot_operator_performance(results_dir="results"):
    """
    Plot operator performance
    
    Args:
        results_dir (str): Directory containing results
    """
    # Load operator performance data
    op_file = os.path.join(results_dir, "operator_performance.csv")
    if not os.path.exists(op_file):
        print(f"Operator performance file not found: {op_file}")
        return
    
    df = load_csv_results(op_file)
    if df is None:
        return
    
    # Ensure we have the expected columns
    if 'caller_operator' not in df.columns:
        df.columns = ['caller_operator', 'total_calls', 'total_duration_seconds',
                     'successful_calls', 'failed_calls', 'success_rate']
    
    # Create directory for plots
    os.makedirs("plots", exist_ok=True)
    
    # Plot success rate by operator
    plt.figure(figsize=(10, 6))
    plt.bar(df['caller_operator'], df['success_rate'] * 100, color='green')
    plt.xlabel('Operator')
    plt.ylabel('Success Rate (%)')
    plt.title('Call Success Rate by Operator')
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.ylim(0, 100)
    plt.savefig("plots/success_rate_by_operator.png")
    plt.close()
    
    # Plot call volume by operator
    plt.figure(figsize=(10, 6))
    plt.bar(df['caller_operator'], df['total_calls'], color='blue')
    plt.xlabel('Operator')
    plt.ylabel('Total Calls')
    plt.title('Call Volume by Operator')
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.savefig("plots/call_volume_by_operator.png")
    plt.close()

def plot_top_users(results_dir="results"):
    """
    Plot top users by call volume and duration
    
    Args:
        results_dir (str): Directory containing results
    """
    # Load user activity data
    user_file = os.path.join(results_dir, "user_activity.csv")
    if not os.path.exists(user_file):
        print(f"User activity file not found: {user_file}")
        return
    
    df = load_csv_results(user_file)
    if df is None:
        return
    
    # Ensure we have the expected columns
    if 'caller_number' not in df.columns:
        df.columns = ['caller_number', 'total_calls', 'total_duration_seconds',
                     'avg_duration_seconds', 'successful_calls', 'total_cost',
                     'voice_calls', 'sms_count', 'data_sessions', 
                     'total_data_usage_kb', 'call_success_rate']
    
    # Sort by total calls and get top 10
    top_by_calls = df.sort_values('total_calls', ascending=False).head(10)
    
    # Sort by total duration and get top 10
    top_by_duration = df.sort_values('total_duration_seconds', ascending=False).head(10)
    
    # Create directory for plots
    os.makedirs("plots", exist_ok=True)
    
    # Plot top users by call volume
    plt.figure(figsize=(12, 6))
    plt.bar(top_by_calls['caller_number'].astype(str), top_by_calls['total_calls'], color='blue')
    plt.xlabel('Phone Number')
    plt.ylabel('Total Calls')
    plt.title('Top 10 Users by Call Volume')
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig("plots/top_users_by_calls.png")
    plt.close()
    
    # Plot top users by duration
    plt.figure(figsize=(12, 6))
    plt.bar(top_by_duration['caller_number'].astype(str), 
            top_by_duration['total_duration_seconds'] / 60, color='green')
    plt.xlabel('Phone Number')
    plt.ylabel('Total Duration (minutes)')
    plt.title('Top 10 Users by Call Duration')
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig("plots/top_users_by_duration.png")
    plt.close()

if __name__ == "__main__":
    # Create plots directory
    os.makedirs("plots", exist_ok=True)
    
    # Generate all plots
    plot_hourly_patterns()
    plot_daily_patterns()
    plot_operator_performance()
    plot_top_users()
    
    print("All plots have been saved to the 'plots' directory.")