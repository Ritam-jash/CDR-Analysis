# CDR Analysis with PySpark

A comprehensive project to analyze Call Detail Records (CDR) using Apache Spark and Python (PySpark).

## Project Overview

This project demonstrates how to use PySpark to analyze large-scale telecom call data records. It provides a complete pipeline from data ingestion to visualization, focusing on identifying usage patterns, user behaviors, and operator performance.

## Features

- **Data Loading**: Efficient loading of CDR data with predefined schema
- **Data Preprocessing**: Data cleaning, type conversion, and quality validation
- **Analysis Tasks**:
  - Call volume analysis (by user, time, operator)
  - Call duration analysis
  - User behavior patterns
  - Operator performance metrics
- **Visualizations**: Call patterns, usage trends, and operator comparisons
- **Output**: CSV exports and visual plots

## Project Structure

```
cdr_analysis_pyspark/
├── data/
│   └── call_logs.csv                  # Sample or real CDR data file
│
├── notebooks/
│   └── cdr_analysis.ipynb             # Jupyter Notebook for step-by-step analysis
│
├── scripts/
│   ├── data_generator.py              # Generate sample CDR data
│   ├── read_data.py                   # Load CSV into PySpark DataFrame
│   ├── preprocess.py                  # Data cleaning, type casting, null handling
│   ├── analyze_calls.py               # Aggregations: total calls, duration, etc.
│   ├── usage_patterns.py              # Time-based analysis: daily/hourly usage
│   ├── visualize_results.py           # Create visualizations of the results 
│   └── main.py                        # Main script to run the complete pipeline
│
├── utils/
│   └── spark_session.py               # Utility to create SparkSession
│
├── plots/                             # Generated visualizations
│
├── results/                           # Analysis output in CSV format
│
├── tests/                             # Unit tests (not implemented in this version)
│
├── requirements.txt                   # Required Python packages
└── README.md                          # Project overview and instructions
```

## Installation

1. Clone this repository:
```bash
git clone https://github.com/yourusername/cdr_analysis_pyspark.git
cd cdr_analysis_pyspark
```

2. Install the required packages:
```bash
pip install -r requirements.txt
```

3. Make sure you have Java installed (required for PySpark)

## Usage

### Generate Sample Data

```bash
python scripts/data_generator.py
```

### Run the Complete Pipeline

```bash
python scripts/main.py
```

This will:
1. Generate sample data
2. Read and preprocess the data
3. Analyze call patterns and user behavior
4. Generate visualizations
5. Save results to the `results` directory

### Run Individual Steps

You can also run each step of the pipeline individually:

```bash
python scripts/read_data.py
python scripts/preprocess.py
python scripts/analyze_calls.py
python scripts/usage_patterns.py
python scripts/visualize_results.py
```

### Interactive Analysis

For interactive analysis, open the Jupyter notebook:

```bash
jupyter lab notebooks/cdr_analysis.ipynb
```

## Sample Visualizations

After running the pipeline, check the `plots` directory for visualizations:

- Call volume by hour of day
- Call types distribution
- Operator performance comparison
- Top users by call volume/duration
- Daily usage patterns

## Future Enhancements

- Real-time CDR processing using Spark Streaming
- Anomaly detection for unusual call patterns
- Predictive analytics for user behavior
- Integration with a dashboard for real-time monitoring
- Geospatial analysis of call patterns


## Acknowledgements

This project is for educational purposes to demonstrate PySpark capabilities for processing and analyzing telecom data.
