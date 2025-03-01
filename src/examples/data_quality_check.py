"""
Example showing how to use the Rule Engine to evaluate data quality.
"""
import argparse
import json
from datetime import datetime

from datacanary.connectors.s3_connector import S3Connector
from datacanary.analysers.statistical_analyser import StatisticalAnalyser
from datacanary.rules.rule_engine import (
    RuleEngine, NullPercentageRule, UniqueValueRule, ValueRangeRule
)
from datacanary.reporting.report_generator import ReportGenerator
from datacanary.utils.logging import setup_logging

def main():
    """Run the example script."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="DataCanary data quality check example")
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    parser.add_argument("--key", required=True, help="S3 object key (path to Parquet file)")
    parser.add_argument("--profile", help="AWS profile name")
    parser.add_argument("--region", help="AWS region")
    parser.add_argument("--report", help="Output file path for the text report")
    parser.add_argument("--json", help="Output file path for the JSON results")
    args = parser.parse_args()
    
    # Set up logging
    setup_logging()
    
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
    df = connector.read_parquet(args.bucket, args.key)
    print(f"Read {len(df)} rows and {len(df.columns)} columns")
    
    # Analyze the data
    print("Analyzing data...")
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