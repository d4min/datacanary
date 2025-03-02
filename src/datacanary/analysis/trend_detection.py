"""
Trend detection module for DataCanary.
Identifies potential anomalies and patterns in data.
"""
import logging
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

class TrendDetector:
    """
    Detects trends and anomolies in dataset statistics.
    """

    def detect_outliers(self, analysis_results):
        """
        Detect outlier values in numeric columns.

        Args:
            analysis_results: Dictionary of column analysis results
        
        Returns:
            Dictionary of outlier information by column
        """
        logger.info("Detecting outliers in dataset")
        
        outliers = {}

        for column_name, column_data in analysis_results.items():
            column_type = column_data.get('type', 'unknown')
            stats = column_data.get('stats', {})
            
            # Only check numeric columns
            if not (column_type.startswith('int') or column_type.startswith('float')):
                continue
            
            # Check if we have the necessary statistics
            if 'mean' not in stats or 'std_dev' not in stats:
                continue
            
            mean = stats['mean']
            std_dev = stats['std_dev']
            min_val = stats.get('min')
            max_val = stats.get('max')
            
            # Skip if std_dev is too small (to avoid division by near-zero)
            if std_dev < 1e-10:
                continue
            
            # Calculate z-scores for min and max
            if min_val is not None:
                min_z_score = abs((min_val - mean) / std_dev)
            else:
                min_z_score = 0
                
            if max_val is not None:
                max_z_score = abs((max_val - mean) / std_dev)
            else:
                max_z_score = 0
            
            # Check for potential outliers (z-score > 3)
            column_outliers = []
            
            if min_z_score > 3:
                column_outliers.append({
                    "value": min_val,
                    "z_score": round(min_z_score, 2),
                    "type": "minimum"
                })
            
            if max_z_score > 3:
                column_outliers.append({
                    "value": max_val,
                    "z_score": round(max_z_score, 2),
                    "type": "maximum"
                })
            
            if column_outliers:
                outliers[column_name] = column_outliers
        
        logger.info(f"Found outliers in {len(outliers)} columns")
        return outliers
    
    def detect_distribution_skewness(self, analysis_results):
        """
        Detect skewness in numeric column distributions.

        Args:
            analysis_results: Dictionary of column analysis results

        Returns:    
            Dictionary of skewness information by column
        """
        skewness_info = {}
        
        for column_name, column_data in analysis_results.items():
            column_type = column_data.get('type', 'unknown')
            stats = column_data.get('stats', {})
            
            # Only check numeric columns
            if not (column_type.startswith('int') or column_type.startswith('float')):
                continue
            
            # Check if we have the necessary statistics
            if 'mean' not in stats or 'median' not in stats:
                continue
            
            mean = stats['mean']
            median = stats['median']
            
            # Calculate simple skewness estimate
            # If mean > median, distribution is right-skewed (positive skewness)
            # If mean < median, distribution is left-skewed (negative skewness)
            if abs(mean - median) < 1e-10:
                skew_direction = "symmetric"
                skew_strength = "none"
            else:
                skew_direction = "right-skewed" if mean > median else "left-skewed"
                
                # Estimate skew strength by difference between mean and median
                diff_percentage = abs(mean - median) / max(abs(mean), abs(median), 1e-10) * 100
                
                if diff_percentage < 5:
                    skew_strength = "mild"
                elif diff_percentage < 15:
                    skew_strength = "moderate"
                else:
                    skew_strength = "strong"
            
            if skew_direction != "symmetric":
                skewness_info[column_name] = {
                    "direction": skew_direction,
                    "strength": skew_strength,
                    "mean": mean,
                    "median": median,
                    "difference_percentage": round(diff_percentage, 2) if 'diff_percentage' in locals() else 0
                }
        
        return skewness_info
    
    def get_data_insights(self, analysis_results):
        """
        Generate insights about the dataset.

        Args:
            analysis_results: Dictionary of column analysis results

        Returns:
            Dictionary of insights
        """
        logger.info("Generating data insights")
        
        # Detect outliers
        outliers = self.detect_outliers(analysis_results)
        
        # Detect skewness
        skewness = self.detect_distribution_skewness(analysis_results)
        
        # Identify columns with high null percentages
        high_null_columns = {}
        for column_name, column_data in analysis_results.items():
            stats = column_data.get('stats', {})
            null_percentage = stats.get('null_percentage', 0)
            
            if null_percentage > 10:  # Consider > 10% nulls as high
                high_null_columns[column_name] = null_percentage
        
        # Identify columns with low uniqueness
        low_unique_columns = {}
        for column_name, column_data in analysis_results.items():
            stats = column_data.get('stats', {})
            unique_percentage = stats.get('unique_percentage', 0)
            
            # Only consider columns with at least 100 rows
            if stats.get('count', 0) >= 100 and unique_percentage < 1:
                low_unique_columns[column_name] = unique_percentage
        
        # Compile insights
        insights = {
            "outliers": outliers,
            "skewness": skewness,
            "data_quality_issues": {
                "high_null_columns": high_null_columns,
                "low_unique_columns": low_unique_columns
            }
        }
        
        # Generate summary and recommendations
        summary = []
        recommendations = []
        
        if outliers:
            summary.append(f"Found potential outliers in {len(outliers)} columns.")
            recommendations.append("Consider investigating outlier values for data entry errors.")
        
        if skewness:
            skewed_columns = sum(1 for info in skewness.values() if info['strength'] in ['moderate', 'strong'])
            if skewed_columns > 0:
                summary.append(f"Found {skewed_columns} columns with significant skewness.")
                recommendations.append("Consider transformations (e.g., log) for strongly skewed numeric columns.")
        
        if high_null_columns:
            summary.append(f"Found {len(high_null_columns)} columns with high null percentages.")
            recommendations.append("Review data collection process for columns with many nulls.")
        
        if low_unique_columns:
            summary.append(f"Found {len(low_unique_columns)} columns with very low uniqueness.")
            recommendations.append("Check if low-uniqueness columns should be categorical rather than continuous.")
        
        insights["summary"] = summary
        insights["recommendations"] = recommendations
        
        logger.info("Data insights generation complete")
        return insights