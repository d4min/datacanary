"""
Summary statistics module for DataCanary.
Provides overall dataset statistics and health metrics.
"""
import logging
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

class SummaryStatistics():
    """
    Generates summary statistics for an entire dataset
    """

    def calculate_summary(self, analysis_results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate summary statistics from analysis results.
        
        Args:
            analysis_results: Dictionary of column analysis results
            
        Returns:
            Dictionary of summary statistics
        """
        logger.info("Calculating dataset summary statistics")
        
        if not analysis_results:
            logger.warning("No analysis results provided for summary calculation")
            return {}
        
        # Initialize counters and statistics
        total_columns = len(analysis_results)
        column_types = {}
        total_null_percentage = 0
        columns_with_nulls = 0
        total_unique_percentage = 0
        highest_null_column = {"name": None, "percentage": 0}
        lowest_unique_column = {"name": None, "percentage": 100}
        
        # Process each column
        for column_name, column_data in analysis_results.items():
            column_type = column_data.get('type', 'unknown')
            stats = column_data.get('stats', {})
            
            # Count column types
            column_types[column_type] = column_types.get(column_type, 0) + 1
            
            # Collect null statistics
            null_percentage = stats.get('null_percentage', 0)
            total_null_percentage += null_percentage
            if null_percentage > 0:
                columns_with_nulls += 1
            if null_percentage > highest_null_column["percentage"]:
                highest_null_column = {"name": column_name, "percentage": null_percentage}
            
            # Collect uniqueness statistics
            unique_percentage = stats.get('unique_percentage', 0)
            total_unique_percentage += unique_percentage
            if unique_percentage < lowest_unique_column["percentage"] and unique_percentage > 0:
                lowest_unique_column = {"name": column_name, "percentage": unique_percentage}
        
        # Calculate averages
        avg_null_percentage = total_null_percentage / total_columns if total_columns > 0 else 0
        avg_unique_percentage = total_unique_percentage / total_columns if total_columns > 0 else 0
        
        # Format the summary
        summary = {
            "dataset_statistics": {
                "total_columns": total_columns,
                "column_types": column_types,
                "columns_with_nulls": columns_with_nulls,
                "columns_with_nulls_percentage": round(columns_with_nulls / total_columns * 100, 2) if total_columns > 0 else 0,
                "avg_null_percentage": round(avg_null_percentage, 2),
                "avg_unique_percentage": round(avg_unique_percentage, 2)
            },
            "data_quality_indicators": {
                "completeness": round(100 - avg_null_percentage, 2),
                "uniqueness": round(avg_unique_percentage, 2)
            },
            "notable_columns": {
                "highest_null_column": highest_null_column,
                "lowest_unique_column": lowest_unique_column
            }
        }
        
        logger.info("Summary statistics calculation complete")
        return summary
    
    def get_health_score(self, analysis_results, rule_results):
        """
        Calculate an overall health score for the dataset.

        Args:
            analysis_results: Dictionary of column analysis results
            rule_results: Dictionary of rule evaluation results
        
        Returns:
            Dictionary with health score and breakdown
        """
        logger.info("Calculating dataset health score")

        # Calculate summary statistics
        summary = self.calculate_summary(analysis_results)
        
        # Calculate rule pass rates
        total_rules = 0
        passed_rules = 0
        rule_scores = {}
        
        for column_name, column_rules in rule_results.items():
            column_total = len(column_rules)
            column_passed = sum(1 for r in column_rules if r['result'].get('passed', False))
            
            total_rules += column_total
            passed_rules += column_passed
            
            # Store column score
            if column_total > 0:
                column_score = round(column_passed / column_total * 100, 2)
                rule_scores[column_name] = column_score
        
        # Calculate overall rule compliance
        rule_compliance = round(passed_rules / total_rules * 100, 2) if total_rules > 0 else 0
        
        # Calculate completeness score (inverse of null percentage)
        completeness = summary['data_quality_indicators']['completeness']
        
        # Calculate overall health score (weighted average)
        health_score = round(
            (rule_compliance * 0.7) +  # 70% weight on rule compliance
            (completeness * 0.3),      # 30% weight on completeness
            2
        )
        
        # Determine health status
        health_status = "Excellent" if health_score >= 90 else \
                        "Good" if health_score >= 75 else \
                        "Fair" if health_score >= 60 else \
                        "Poor"
        
        # Create health report
        health_report = {
            "health_score": health_score,
            "health_status": health_status,
            "components": {
                "rule_compliance": rule_compliance,
                "completeness": completeness
            },
            "column_scores": rule_scores
        }
        
        logger.info(f"Health score calculation complete: {health_score} ({health_status})")
        return health_report

