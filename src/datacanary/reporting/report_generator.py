"""
Report generation module for DataCanary.
Formats analysis and rule results into readable reports.
"""
import logging 
from datetime import datetime

logger = logging.getLogger(__name__)

class ReportGenerator:
    """
    Generates formatted reports from analysis and rule evaluation results.
    """
    
    def generate_text_report(self, dataset_name, analysis_results, rule_results):
        """
        Generate a text report of data quality issues.
        
        Args:
            dataset_name: Name or identifier of the dataset
            analysis_results: Dictionary of analysis results from StatisticalAnalyser
            rule_results: Dictionary of rule results from RuleEngine

        Returns:
            str: Formatted report text
        """
        logger.info(f"Generating text report for dataset: {dataset_name}")
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        report = [
            f"= DataCanary Quality Report =",
            f"Dataset: {dataset_name}",
            f"Generated: {now}",
            f"Total columns: {len(analysis_results)}",
            ""
        ]
        
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
        
        logger.info(f"Generated report with {len(analysis_results)} columns and {total_rules} rules")
        return "\n".join(report)