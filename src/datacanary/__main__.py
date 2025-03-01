"""
Main entry point for the DataCanary command-line tool.
"""
import argparse
import json
import sys
import logging

from datacanary.connectors.s3_connector import S3Connector
from datacanary.analysers.statistical_analyser import StatisticalAnalyser
from datacanary.rules.rule_engine import (
    RuleEngine, NullPercentageRule, UniqueValueRule, ValueRangeRule
)
from datacanary.reporting.report_generator import ReportGenerator
from datacanary.utils.logging import setup_logging

def main():
    """Main entry point for the DataCanary CLI."""
    # Set up argument parser
    parser = argparse.ArgumentParser(description="DataCanary - Data Quality Tool")
    
    # Create subparsers for different commands
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # 'analyse' command
    analyse_parser = subparsers.add_parser("analyse", help="analyse data statistics")
    analyse_parser.add_argument("--bucket", required=True, help="S3 bucket name")
    analyse_parser.add_argument("--key", required=True, help="S3 object key (path to Parquet file)")
    analyse_parser.add_argument("--profile", help="AWS profile name")
    analyse_parser.add_argument("--region", help="AWS region")
    analyse_parser.add_argument("--output", help="Output file path for JSON results")

    # 'check' command
    check_parser = subparsers.add_parser("check", help="Run data quality checks")
    check_parser.add_argument("--bucket", required=True, help="S3 bucket name")
    check_parser.add_argument("--key", required=True, help="S3 object key (path to Parquet file)")
    check_parser.add_argument("--profile", help="AWS profile name")
    check_parser.add_argument("--region", help="AWS region")
    check_parser.add_argument("--report", help="Output file path for the text report")
    check_parser.add_argument("--json", help="Output file path for the JSON results")

    # Parse arguments
    args = parser.parse_args()
    
    # Set up logging
    setup_logging()
    
    # Handle the case when no command is provided
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    # Process commands
    if args.command == "analyse":
        run_analyse(args)
    elif args.command == "check":
        run_check(args)

def run_analyse(args):
    """Run the analyse command."""
    # Create S3 connector and analyser
    connector = S3Connector(
        aws_profile=args.profile,
        aws_region=args.region
    )
    analyser = StatisticalAnalyser()

    # Read the data from S3
    print(f"Reading data from s3://{args.bucket}/{args.key}")
    try:
        df = connector.read_parquet(args.bucket, args.key)
        print(f"Read {len(df)} rows and {len(df.columns)} columns")
    except Exception as e:
        print(f"Error reading data: {e}")
        sys.exit(1)
    
    # Analyze the data
    print("Analysing data...")
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

def run_check(args):
    """Run the check command."""
    # Create components
    connector = S3Connector(
        aws_profile=args.profile,
        aws_region=args.region
    )
    analyser = StatisticalAnalyser()
    engine = RuleEngine()
    reporter = ReportGenerator()
    
    # Add rules to the engine
    engine.add_rule(NullPercentageRule(threshold=5.0))
    engine.add_rule(UniqueValueRule(threshold=90.0))
    engine.add_rule(ValueRangeRule(min_value=0))

    # Read the data from S3
    print(f"Reading data from s3://{args.bucket}/{args.key}")
    try:
        df = connector.read_parquet(args.bucket, args.key)
        print(f"Read {len(df)} rows and {len(df.columns)} columns")
    except Exception as e:
        print(f"Error reading data: {e}")
        sys.exit(1)
    
    # Analyze the data
    print("Analysing data...")
    analysis_results = analyser.analyse_dataframe(df)
    
    # Evaluate the rules
    print("Evaluating data quality rules...")
    rule_results = engine.evaluate_dataframe(analysis_results)
    
    # Generate a report
    report = reporter.generate_text_report(args.key, analysis_results, rule_results)
    print("\nData Quality Report:")
    print(report)
    
    # Save report to a file if requested
    if args.report:
        print(f"Saving report to {args.report}")
        with open(args.report, 'w') as f:
            f.write(report)
    
    # Save results to a JSON file if requested
    if args.json:
        print(f"Saving JSON results to {args.json}")
        from datetime import datetime
        results = {
            "dataset": args.key,
            "timestamp": datetime.now().isoformat(),
            "analysis": analysis_results,
            "rule_results": rule_results
        }
        with open(args.json, 'w') as f:
            json.dump(results, f, indent=2, default=str)

if __name__ == "__main__":
    main()