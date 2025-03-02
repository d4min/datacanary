"""
Main entry point for the DataCanary command-line tool.
"""
import argparse
import json
import sys
import logging

from datacanary import __version__

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
    parser.add_argument('--version', action='version', version=f'DataCanary v{__version__}')
    
    # Create subparsers for different commands
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # 'analyse' command
    analyse_parser = subparsers.add_parser("analyse", help="analyse data statistics")
    # s3
    analyse_parser.add_argument("--bucket", required=True, help="S3 bucket name")
    analyse_parser.add_argument("--key", required=True, help="S3 object key (path to Parquet file)")
    analyse_parser.add_argument("--profile", help="AWS profile name")
    analyse_parser.add_argument("--region", help="AWS region")
    # azure
    analyse_parser.add_argument("--azure-container", help="Azure Blob Storage container name")
    analyse_parser.add_argument("--azure-blob", help="Azure Blob Storage blob name (path to Parquet file)")
    analyse_parser.add_argument("--azure-connection-string", help="Azure Storage connection string")
    analyse_parser.add_argument("--azure-account-url", help="Azure Storage account URL")
    analyse_parser.add_argument("--azure-account-key", help="Azure Storage account key")
    #generic
    analyse_parser.add_argument("--output", help="Output file path for JSON results")

    # 'analyse-local' command for local files
    analyse_local_parser = subparsers.add_parser("analyse-local", help="analyse data statistics for local file")
    analyse_local_parser.add_argument("--file", required=True, help="Path to local Parquet file")
    analyse_local_parser.add_argument("--output", help="Output file path for JSON results")

    # 'check' command
    check_parser = subparsers.add_parser("check", help="Run data quality checks")
    # s3
    check_parser.add_argument("--bucket", required=True, help="S3 bucket name")
    check_parser.add_argument("--key", required=True, help="S3 object key (path to Parquet file)")
    check_parser.add_argument("--profile", help="AWS profile name")
    check_parser.add_argument("--region", help="AWS region")
    # azure
    check_parser.add_argument("--azure-container", help="Azure Blob Storage container name")
    check_parser.add_argument("--azure-blob", help="Azure Blob Storage blob name (path to Parquet file)")
    check_parser.add_argument("--azure-connection-string", help="Azure Storage connection string")
    check_parser.add_argument("--azure-account-url", help="Azure Storage account URL")
    check_parser.add_argument("--azure-account-key", help="Azure Storage account key")
    #generic
    check_parser.add_argument("--report", help="Output file path for the text report")
    check_parser.add_argument("--json", help="Output file path for the JSON results")
    check_parser.add_argument("--rules", help="Path to rule configuration file (YAML or JSON)")

    # 'check-local' command for local files
    check_local_parser = subparsers.add_parser("check-local", help="Run data quality checks on local file")
    check_local_parser.add_argument("--file", required=True, help="Path to local Parquet file")
    check_local_parser.add_argument("--report", help="Output file path for the text report")
    check_local_parser.add_argument("--json", help="Output file path for the JSON results")
    check_local_parser.add_argument("--rules", help="Path to rule configuration file (YAML or JSON)")

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
    elif args.command == "analyse-local":
        run_analyse_local(args)
    elif args.command == "check-local":
        run_check_local(args)

def run_analyse(args):
    """Run the analyse command."""
    # Determine which connector to use
    if args.bucket and args.key:
        # Use S3 connector
        connector = S3Connector(
            aws_profile=args.profile,
            aws_region=args.region
        )
        data_source = f"s3://{args.bucket}/{args.key}"
        try:
            df = connector.read_parquet(args.bucket, args.key)
        except Exception as e:
            print(f"Error reading data from S3: {e}")
            sys.exit(1)
    elif args.azure_container and args.azure_blob:
        # Use Azure connector
        from datacanary.connectors.azure_connector import AzureConnector
        try:
            connector = AzureConnector(
                connection_string=args.azure_connection_string,
                account_url=args.azure_account_url,
                account_key=args.azure_account_key
            )
            data_source = f"azure://{args.azure_container}/{args.azure_blob}"
            df = connector.read_parquet(args.azure_container, args.azure_blob)
        except Exception as e:
            print(f"Error reading data from Azure: {e}")
            sys.exit(1)
    else:
        print("Error: You must specify either S3 (--bucket and --key) or Azure (--azure-container and --azure-blob) source")
        sys.exit(1)
    analyser = StatisticalAnalyser()
    
    # analyse the data
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
    # Determine which connector to use
    if args.bucket and args.key:
        # Use S3 connector
        connector = S3Connector(
            aws_profile=args.profile,
            aws_region=args.region
        )
        data_source = f"s3://{args.bucket}/{args.key}"
        try:
            df = connector.read_parquet(args.bucket, args.key)
        except Exception as e:
            print(f"Error reading data from S3: {e}")
            sys.exit(1)
    elif args.azure_container and args.azure_blob:
        # Use Azure connector
        from datacanary.connectors.azure_connector import AzureConnector
        try:
            connector = AzureConnector(
                connection_string=args.azure_connection_string,
                account_url=args.azure_account_url,
                account_key=args.azure_account_key
            )
            data_source = f"azure://{args.azure_container}/{args.azure_blob}"
            df = connector.read_parquet(args.azure_container, args.azure_blob)
        except Exception as e:
            print(f"Error reading data from Azure: {e}")
            sys.exit(1)
    else:
        print("Error: You must specify either S3 (--bucket and --key) or Azure (--azure-container and --azure-blob) source")
        sys.exit(1)
    analyser = StatisticalAnalyser()
    engine = RuleEngine()
    reporter = ReportGenerator()
    
    # Add rules to the engine
    if args.rules:
        from datacanary.config.rule_config import apply_rules_to_engine
        apply_rules_to_engine(engine, args.rules)
    else:
        # Add default rules
        engine.add_rule(NullPercentageRule(threshold=5.0))
        engine.add_rule(UniqueValueRule(threshold=90.0))
        engine.add_rule(ValueRangeRule(min_value=0))
    
    # analyse the data
    print("Analysing data...")
    analysis_results = analyser.analyse_dataframe(df)
    
    # Evaluate the rules
    print("Evaluating data quality rules...")
    rule_results = engine.evaluate_dataframe(analysis_results)

    # After rule evaluation, generate insights
    from datacanary.analysis.summary_statistics import SummaryStatistics
    from datacanary.analysis.trend_detection import TrendDetector

    # Get summary statistics and health score
    summary_stats = SummaryStatistics().calculate_summary(analysis_results)
    health_score = SummaryStatistics().get_health_score(analysis_results, rule_results)

    # Get insights
    insights = TrendDetector().get_data_insights(analysis_results)

    # Print health score and insights
    print(f"\nHealth Score: {health_score['health_score']} ({health_score['health_status']})")
    
    if insights['summary']:
        print("\nData Insights:")
        for insight in insights['summary']:
            print(f"- {insight}")
    
    if insights['recommendations']:
        print("\nRecommendations:")
        for recommendation in insights['recommendations']:
            print(f"- {recommendation}")
    
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

def run_analyse_local(args):
    """
    Run the analyse command on a local file 
    """
    analyser = StatisticalAnalyser()

    print(f"Reading data from {args.file}")
    try:
        import pandas as pd
        df = pd.read_parquet(args.file)
        print(f"Read {len(df)} rows and {len(df.columns)} columns")
    except Exception as e:
        print(f"Error reading data: {e}")
        sys.exit(1)
    
    # analyse the data
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

def run_check_local(args):
    """
    Run the check command on a local file.
    """
    analyser = StatisticalAnalyser()
    engine = RuleEngine()
    reporter = ReportGenerator()
    
    # Add rules to the engine
    if args.rules:
        from datacanary.config.rule_config import apply_rules_to_engine
        apply_rules_to_engine(engine, args.rules)
    else:
        # Add default rules
        engine.add_rule(NullPercentageRule(threshold=5.0))
        engine.add_rule(UniqueValueRule(threshold=90.0))
        engine.add_rule(ValueRangeRule(min_value=0))
    
    # Read the local file
    print(f"Reading data from {args.file}")
    try:
        import pandas as pd
        df = pd.read_parquet(args.file)
        print(f"Read {len(df)} rows and {len(df.columns)} columns")
    except Exception as e:
        print(f"Error reading data: {e}")
        sys.exit(1)

    # analyse the data
    print("Analysing data...")
    analysis_results = analyser.analyse_dataframe(df)
    
    # Evaluate the rules
    print("Evaluating data quality rules...")
    rule_results = engine.evaluate_dataframe(analysis_results)

    # After rule evaluation, generate insights
    from datacanary.analysis.summary_statistics import SummaryStatistics
    from datacanary.analysis.trend_detection import TrendDetector
    
    # Get summary statistics and health score
    summary_stats = SummaryStatistics().calculate_summary(analysis_results)
    health_score = SummaryStatistics().get_health_score(analysis_results, rule_results)
    
    # Get insights
    insights = TrendDetector().get_data_insights(analysis_results)
    
    # Print health score and insights
    print(f"\nHealth Score: {health_score['health_score']} ({health_score['health_status']})")
    
    if insights['summary']:
        print("\nData Insights:")
        for insight in insights['summary']:
            print(f"- {insight}")
    
    if insights['recommendations']:
        print("\nRecommendations:")
        for recommendation in insights['recommendations']:
            print(f"- {recommendation}")
    
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