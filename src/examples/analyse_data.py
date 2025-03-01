"""
Example showing how to use the StatisticalAnalyser
"""
import argparse
import json 

from datacanary.connectors.s3_connector import S3Connector
from datacanary.analysers.statistical_analyser import StatisticalAnalyser
from datacanary.utils.logging import setup_logging

def main():
    """
    Run the example script.
    """
    parser = argparse.ArgumentParser(description="DataCanary data analysis example")
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    parser.add_argument("--key", required=True, help="S3 object key (path to Parquet file)")
    parser.add_argument("--profile", help="AWS profile name")
    parser.add_argument("--region", default="None", help="AWS region")
    parser.add_argument("--output", help="Output file path for the analysis results (JSON)")
    args = parser.parse_args()

    setup_logging()

    connector = S3Connector(
        aws_profile = args.profile,
        aws_region = args.region
    )
    analyser = StatisticalAnalyser()

    # Read the data from S3
    print(f"Reading data from s3://{args.bucket}/{args.key}")
    df = connector.read_parquet(args.bucket, args.key)
    print(f"Read {len(df)} rows and {len(df.columns)} columns")

    # Analyse the data
    print('Analysing data...')
    results = analyser.analyse_dataframe(df)

        # Display overview of the analysis
    print("\nAnalysis Results Overview:")
    print("-" * 50)
    for column, analysis in results.items():
        stats = analysis['stats']
        dtype = analysis['type']
        
        null_pct = stats['null_percentage']
        unique_pct = stats.get('unique_percentage', 'N/A')
        
        print(f"Column: {column}")
        print(f"  Type: {dtype}")
        print(f"  Nulls: {stats['null_count']} ({null_pct}%)")
        print(f"  Unique Values: {stats['unique_count']} ({unique_pct}%)")
        
        if 'min' in stats:
            print(f"  Range: {stats['min']} to {stats['max']}")
        elif 'min_length' in stats:
            print(f"  String Length: {stats['min_length']} to {stats['max_length']}")
        elif 'min_date' in stats:
            print(f"  Date Range: {stats['min_date']} to {stats['max_date']}")
        
        print("-" * 50)
    
    # Save results to a file if requested
    if args.output:
        print(f"Saving analysis results to {args.output}")
        with open(args.output, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        print("Results saved successfully")

if __name__ == "__main__":
    main()
