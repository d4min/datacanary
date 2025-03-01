"""
Statistical analyzer for DataCanary.
Performs basic statistical analysis on DataFrame columns.
"""
import logging
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

class StatisticalAnalyser:
    """
    Performs statistical analysis on DataFrame columns to identify
    potential data quality issues.
    """

    def analyse_dataframe(self, df):
        """
        Analyse a pandas DataFrame and return statistics for each column.

        Args:
            df: The DataFrame to to analyse

        Returns:
            dict: Dictionary of column statistics
        """
        if not isinstance(df, pd.DataFrame):
            logger.error("Input is not a pandas DataFrame")
            raise TypeError("Input must be a pandas DataFrame")
        
        if df.empty:
            logger.warning("Empty DataFrame provided for analysis")
            return {}
    
        logger.info(f"Analyzing DataFrame with {len(df)} rows and {len(df.columns)} columns")

        results = {}

        for column in df.columns:
            logger.debug(f'Analysing column: {column}')
            col_data = df[column]

            stats = {
                "count": len(col_data),
                "null_count": col_data.isna().sum(),
                "null_percentage": round(col_data.isna().mean() * 100, 2),
                "unique_count": col_data.nunique(),
                "unique_percentage": round(col_data.nunique() / len(col_data) * 100, 2) if len(col_data) > 0 else 0,
                "has_duplicates": col_data.duplicated().any()
            }

            if pd.api.types.is_numeric_dtype(col_data):
                non_null_data = col_data.dropna()
                if len(non_null_data) > 0:
                    stats.update({
                        "min": float(non_null_data.min()),
                        "max": float(non_null_data.max()),
                        "mean": float(non_null_data.mean()),
                        "median": float(non_null_data.median()),
                        "std_dev": float(non_null_data.std()) if len(non_null_data) > 1 else 0,
                        "zeros_count": (non_null_data == 0).sum(),
                        "zeros_percentage": round((non_null_data == 0).mean() * 100, 2),
                        "negative_count": (non_null_data < 0).sum()
                    })

            elif pd.api.types.is_string_dtype(col_data) or pd.api.types.is_object_dtype(col_data):
                non_null_data = col_data.dropna()
                if len(non_null_data) > 0:
                    # Calculate string lengths
                    str_lengths = non_null_data.astype(str).str.len()
                    stats.update({
                        "min_length": int(str_lengths.min()),
                        "max_length": int(str_lengths.max()),
                        "mean_length": float(str_lengths.mean()),
                        "empty_string_count": (non_null_data == "").sum(),
                        "empty_string_percentage": round((non_null_data == "").mean() * 100, 2)
                    })

            elif pd.api.types.is_datetime64_dtype(col_data):
                non_null_data = col_data.dropna()
                if len(non_null_data) > 0:
                    stats.update({
                        "min_date": non_null_data.min().strftime("%Y-%m-%d %H:%M:%S"),
                        "max_date": non_null_data.max().strftime("%Y-%m-%d %H:%M:%S"),
                        "range_days": (non_null_data.max() - non_null_data.min()).days
                    })

            # Store results for this column
            results[column] = {
                "type": str(col_data.dtype),
                "stats": stats
            }
        
        logger.info(f"Completed analysis of {len(results)} columns")
        return results