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
from datacanary.utils.logging import setup_logging

def generate_report(df_name, analysis_results, rule_results):
    """
    Generate a simple text report of rule evaluation results.

    Args:
        df_name: Name of the DataFrame being analysed
        analysis_results: Dictionary of analysis results
        rule_resulsts: Dictionary of rule evaluation results

    Returns:
        str: Formatted report text
    """
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    report = [
        f"= DataCanary Quality Report =",
        f"Dataset: {df_name}",
        f"Generated: {now}",
        f"Total columns: {len(analysis_results)}",
        ""
    ]

    total_rules = 0
    passed_rules = 0

    for column_name, column_rules in rule_results.items():
        # Get column statistics
        column_stats = analysis_results.get(column_name, {}).get('stats', {})
        column_type = analysis_results.get(column_name, {}).get('type', 'unknown')
        
        # Count passed and failed rules
        column_passed = sum(1 for r in column_rules if r['result'].get('passed', False))
        column_total = len(column_rules)
        
        total_rules += column_total
        passed_rules += column_passed
        
        # Add column section
        status = "✓" if column_passed == column_total else "✗"
        report.append(f"== Column: {column_name} [{status}] ==")
        report.append(f"Type: {column_type}")
        report.append(f"Rules: {column_passed}/{column_total} passed")
        
        # Add statistics
        report.append("Statistics:")
        for stat_name, stat_value in column_stats.items():
            report.append(f"  {stat_name}: {stat_value}")
        
        # Add rule results
        report.append("Rule Results:")
        for rule in column_rules:
            rule_name = rule['rule_name']
            rule_desc = rule['description']
            rule_result = rule['result']
            passed = rule_result.get('passed', False)
            message = rule_result.get('message', 'No details')
            
            status = "✓" if passed else "✗"
            report.append(f"  [{status}] {rule_name}: {message}")
        
        report.append("")
    
    # Add summary
    pass_rate = (passed_rules / total_rules * 100) if total_rules > 0 else 0
    report.append(f"== Summary ==")
    report.append(f"Total rules evaluated: {total_rules}")
    report.append(f"Rules passed: {passed_rules} ({pass_rate:.1f}%)")
    report.append(f"Overall status: {'PASSED' if pass_rate == 100 else 'FAILED'}")
    
    return "\n".join(report)

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
    
    # Create S3 connector, analyser, and rule engine
    connector = S3Connector(
        aws_profile=args.profile,
        aws_region=args.region
    )
    analyser = StatisticalAnalyser()
    engine = RuleEngine()
    
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
    report = generate_report(args.key, analysis_results, rule_results)
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