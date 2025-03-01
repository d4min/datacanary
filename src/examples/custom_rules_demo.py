"""
Example showing how to use custom rule definitions.
"""
import os
import sys
import argparse
import pandas as pd

# Add the src directory to the Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.insert(0, os.path.join(project_root, "src"))

from datacanary.analysers.statistical_analyser import StatisticalAnalyser
from datacanary.rules.rule_engine import RuleEngine
from datacanary.config.rule_config import apply_rules_to_engine
from datacanary.reporting.report_generator import ReportGenerator
from datacanary.utils.logging import setup_logging

def main():
    """Run the custom rules demo."""
    parser = argparse.ArgumentParser(description="DataCanary Custom Rules Demo")
    parser.add_argument("--file", required=True, help="Path to Parquet file")
    parser.add_argument("--rules", required=True, help="Path to rule configuration file")
    parser.add_argument("--report", help="Output file path for the text report")
    args = parser.parse_args()
    
    # Set up logging
    setup_logging()
    
    # Create components
    analyser = StatisticalAnalyser()
    engine = RuleEngine()
    reporter = ReportGenerator()
    
    # Load and apply custom rules
    apply_rules_to_engine(engine, args.rules)
    
    # Read the data
    print(f"Reading data from {args.file}")
    try:
        df = pd.read_parquet(args.file)
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
    
    # Generate and display a report
    report = reporter.generate_text_report(
        dataset_name=args.file,
        analysis_results=analysis_results,
        rule_results=rule_results,
        output_path=args.report
    )
    
    print("\nData Quality Report:")
    print(report)

if __name__ == "__main__":
    main()