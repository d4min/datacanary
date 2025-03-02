"""
Report generation module for DataCanary.
Formats analysis and rule results into readable reports.
"""
import logging
import os
from datetime import datetime
import re

logger = logging.getLogger(__name__)

class ReportGenerator:
    """
    Generates formatted reports from analysis and rule evaluation results.
    """

    def __init__(self):
        """
        Initialise the report generator
        """
        # Use the fixed directory path
        current_dir = os.path.dirname(os.path.abspath(__file__))
        self.reports_dir = os.path.join(current_dir, "reports")
        
        # Ensure the reports directory exists
        if not os.path.exists(self.reports_dir):
            os.makedirs(self.reports_dir)
            logger.info(f"Created reports directory: {self.reports_dir}")

    def _get_report_filename(self, dataset_name):
        """
        Generate a standardized filename for a report.
        
        Args:
            dataset_name: Name or identifier of the dataset
            
        Returns:
            str: Standardised filename
        """
        # Extract filename from S3 path
        file_name = os.path.basename(dataset_name)
        # Remove extension if present
        file_name = os.path.splitext(file_name)[0]
        # Clean up the filename (remove any characters that aren't suitable for filenames)
        file_name = re.sub(r'[^\w\-_]', '_', file_name)
        # Current date and time in a filename-friendly format
        date_str = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        return f"datacanary_report_{file_name}_{date_str}.txt"
    
    def generate_text_report(self, dataset_name, analysis_results, rule_results, output_path=None):
        """
        Generate a text report of data quality results.
        
        Args:
            dataset_name: Name or identifier of the dataset
            analysis_results: Dictionary of analysis results from StatisticalAnalyzer
            rule_results: Dictionary of rule evaluation results from RuleEngine
            output_path: Optional custom path to save the report
            
        Returns:
            str: Formatted report text
        """
        logger.info(f"Generating text report for dataset: {dataset_name}")
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Generate summary statistics and insights
        from datacanary.analysis.summary_statistics import SummaryStatistics
        from datacanary.analysis.trend_detection import TrendDetector

        summary_stats = SummaryStatistics().calculate_summary(analysis_results)
        health_score = SummaryStatistics().get_health_score(analysis_results, rule_results)
        insights = TrendDetector().get_data_insights(analysis_results)

        report = [
            f"= DataCanary Quality Report =",
            f"Dataset: {dataset_name}",
            f"Generated: {now}",
            f"Total columns: {len(analysis_results)}",
            f"Health Score: {health_score['health_score']} ({health_score['health_status']})",
            ""
        ]

        # Add dataset summary section
        report.append("== Dataset Summary ==")
        dataset_stats = summary_stats['dataset_statistics']
        report.append(f"Total columns: {dataset_stats['total_columns']}")

        # Format column types
        column_types_str = ", ".join([f"{type}: {count}" for type, count in dataset_stats['column_types'].items()])
        report.append(f"Column types: {column_types_str}")

        report.append(f"Columns with nulls: {dataset_stats['columns_with_nulls']} ({dataset_stats['columns_with_nulls_percentage']}%)")
        report.append(f"Average null percentage: {dataset_stats['avg_null_percentage']}%")
        report.append(f"Average unique percentage: {dataset_stats['avg_unique_percentage']}%")
        report.append("")

        # Add data insights section
        if insights['summary']:
            report.append("== Data Insights ==")
            for insight in insights['summary']:
                report.append(f"- {insight}")
            report.append("")

        # Add recommendations section
        if insights['recommendations']:
            report.append("== Recommendations ==")
            for recommendation in insights['recommendations']:
                report.append(f"- {recommendation}")
            report.append("")

        # Overall statistics
        total_rules = 0
        passed_rules = 0
        
        # Process each column
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
        
        # Convert to string
        report_text = "\n".join(report)
        
        # Save the report (always save to the fixed directory)
        filename = self._get_report_filename(dataset_name)
        default_path = os.path.join(self.reports_dir, filename)
        
        # If custom output path provided, use it as well
        save_paths = [default_path]
        if output_path:
            save_paths.append(output_path)
        
        # Save to all paths
        for path in save_paths:
            try:
                with open(path, 'w') as f:
                    f.write(report_text)
                logger.info(f"Report saved to: {path}")
            except Exception as e:
                logger.error(f"Error saving report to {path}: {e}")
        
        return report_text