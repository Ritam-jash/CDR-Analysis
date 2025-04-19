import csv
import random
import os
from datetime import datetime, timedelta

def generate_sample_cdr_data(output_file="data/call_logs.csv", num_records=10000):
    """
    Generate sample CDR data and write to CSV file
    
    Args:
        output_file (str): Path to output CSV file
        num_records (int): Number of CDR records to generate
    """
    # Create the directory if it doesn't exist
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Define phone number prefixes for different operators
    operator_prefixes = {
        'Operator A': ['701', '702', '703'],
        'Operator B': ['801', '802', '803'],
        'Operator C': ['901', '902', '903']
    }
    
    # Define call types
    call_types = ['VOICE', 'SMS', 'DATA']
    
    # Generate random CDR records
    records = []
    
    # Start date (7 days ago)
    start_date = datetime.now() - timedelta(days=7)
    
    for _ in range(num_records):
        # Select random operator for caller and callee
        caller_operator = random.choice(list(operator_prefixes.keys()))
        callee_operator = random.choice(list(operator_prefixes.keys()))
        
        # Generate random phone numbers
        caller_prefix = random.choice(operator_prefixes[caller_operator])
        caller_number = f"{caller_prefix}{random.randint(1000000, 9999999)}"
        
        callee_prefix = random.choice(operator_prefixes[callee_operator])
        callee_number = f"{callee_prefix}{random.randint(1000000, 9999999)}"
        
        # Generate random timestamp within the last week
        random_seconds = random.randint(0, 7 * 24 * 60 * 60)
        timestamp = start_date + timedelta(seconds=random_seconds)
        
        # Call type
        call_type = random.choice(call_types)
        
        # Call duration (0 for failed calls, SMS is usually short)
        if call_type == 'VOICE':
            duration = random.randint(0, 3600)  # 0 to 60 minutes
            data_usage = 0
        elif call_type == 'SMS':
            duration = random.randint(1, 5)
            data_usage = 0
        else:  # DATA
            duration = random.randint(60, 7200)  # 1 minute to 2 hours
            data_usage = random.randint(1, 1000)  # 1KB to 1000KB
        
        # Call status
        status = random.choices(['ANSWERED', 'FAILED', 'BUSY'], weights=[0.8, 0.15, 0.05])[0]
        
        # Cost (based on duration and type)
        if call_type == 'VOICE':
            cost = round(duration * 0.01, 2)  # $0.01 per second
        elif call_type == 'SMS':
            cost = 0.5  # Flat rate for SMS
        else:  # DATA
            cost = round(data_usage * 0.005, 2)  # $0.005 per KB
            
        record = [
            caller_number,
            callee_number,
            timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            call_type,
            duration,
            status,
            caller_operator,
            callee_operator,
            data_usage,
            cost
        ]
        records.append(record)
    
    # Write to CSV
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            'caller_number', 'callee_number', 'timestamp', 'call_type',
            'duration', 'status', 'caller_operator', 'callee_operator', 
            'data_usage_kb', 'cost'
        ])
        writer.writerows(records)
    
    print(f"Generated {num_records} sample CDR records in {output_file}")

if __name__ == "__main__":
    generate_sample_cdr_data()