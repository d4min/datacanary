"""
Rule engine for DataCanary.
Defines and evaluates data quality rules.
"""
import logging

logger = logging.getLogger(__name__)

class Rule:
    """
    Base class for data quality rules.
    """
    def __init__(self, name, description, applicable_types):
        """
        Initialise a rule.

        Args:
            name: Name of the rule
            description: Description of the rule
            applicable_types: List of data types this rule applies to (None means all types)
        """
        self.name = name
        self.description = description
        self.applicable_types = applicable_types

    def is_applicable(self, column_stats):
        """
        Check if this rule is applicable to the given column.

        Args:
            column_stats: Dictionary of statistics for a column

        Returns:
            bool: True if the rule is applicable, False if not
        """
        if self.applicable_types is None:
            return True

        if 'type' not in column_stats:
            return False
        
        col_type = column_stats['type']

        for applicable_type in self.applicable_types:
            if isinstance(applicable_type, str) and col_type.startswith(applicable_type):
                return True

    def evaluate(self, column_stats):
        """
        Evaluate a rule against column statistics.

        Args:
            column_stats: Dictionary of statistics for a column 

        Returns:
            dict: Evaluation result

        Raises:
            NotImplementedError: If the subclass doesn't implement this method
        """
        raise NotImplementedError("Subclasses must implement this method")
    
class NullPercentageRule(Rule):
    """
    Rule to check if null percentage is below a threshold.
    """
    def __init__(self, threshold=5.0):
        """
        Initialise the rule.

        Args:
            threshold: Maximum acceptable percentage of null values.
        """
        super().__init__(
            name = "null_percentage_check",
            description = f"Check if null percentage is below {threshold}%",
            applicable_types=None  # Applies to all column types
        )
        self.threshold = threshold

    def evaluate(self, column_stats):
        """
        Evaluate if null percentage is below threshold.
        
        Args:
            column_stats: Dictionary of statistics for a column
            
        Returns:
            dict: Evaluation result
        """
        if 'stats' not in column_stats or 'null_percentage' not in column_stats['stats']:
            logger.warning(f"Cannot evaluate {self.name}: required statistics not found")
            return {
                "passed": False,
                "reason": "Required statistics not available",
                "details": "Missing 'null_percentage' statistic"
            }
        
        null_pct = column_stats['stats']['null_percentage']
        passed = null_pct <= self.threshold
        
        return {
            "passed": passed,
            "actual": null_pct,
            "threshold": self.threshold,
            "message": f"Column has {null_pct:.2f}% nulls (threshold: {self.threshold}%)"
        }
    
class UniqueValueRule(Rule):
    """
    Rule to check if a column has a minimum percentage of unique values.
    """
    def __init__(self, threshold = 90.0):
        """
        Initialise the rule. 

        Args:
            threshold: Minimmum acceptable percentage of unique values
        """
        super().__init__(
            name="unique_value_check",
            description=f"Check if unique value percentage is at least {threshold}%",
            applicable_types=None  # Applies to all column types
        )
        self.threshold = threshold

    def evaluate(self, column_stats):
        """
        Evaluate if unique value percentage meets threshold.
        
        Args:
            column_stats: Dictionary of statistics for a column
            
        Returns:
            dict: Evaluation result
        """
        if 'stats' not in column_stats or 'unique_percentage' not in column_stats['stats']:
            logger.warning(f"Cannot evaluate {self.name}: required statistics not found")
            return {
                "passed": False,
                "reason": "Required statistics not available",
                "details": "Missing 'unique_percentage' statistic"
            }
        
        unique_pct = column_stats['stats']['unique_percentage']
        passed = unique_pct >= self.threshold
        
        return {
            "passed": passed,
            "actual": unique_pct,
            "threshold": self.threshold,
            "message": f"Column has {unique_pct:.2f}% unique values (threshold: {self.threshold}%)"
        }
    
class ValueRangeRule(Rule):
    """
    Rule to check if numeric values are within a specified range.
    """
    def __init__(self, min_value=None, max_value=None):
        """
        Initialise the rule.

        Args:
            min_value: Minimum acceptable value (None means no minimum)
            max_value: Maximum acceptable value (None means no maximum)
        """
        description = "Check if values are within range"
        if min_value is not None and max_value is not None:
            description = f"Check if values are between {min_value} and {max_value}"
        elif min_value is not None:
            description = f"Check if values are at least {min_value}"
        elif max_value is not None:
            description = f"Check if values are at most {max_value}"
            
        super().__init__(
            name="value_range_check",
            description=description,
            applicable_types=["int", "float", "numeric"]  # Only applies to numeric columns
        )
        self.min_value = min_value
        self.max_value = max_value
    
    def evaluate(self, column_stats):
        """
        Evaluate if values are within specified range.
        
        Args:
            column_stats: Dictionary of statistics for a column
            
        Returns:
            dict: Evaluation result
        """
        # Check if required stats are available
        if 'stats' not in column_stats or 'min' not in column_stats['stats'] or 'max' not in column_stats['stats']:
            logger.warning(f"Cannot evaluate {self.name}: required statistics not found")
            return {
                "passed": False,
                "reason": "Required statistics not available",
                "details": "Missing 'min' or 'max' statistics"
            }
        
        min_val = column_stats['stats']['min']
        max_val = column_stats['stats']['max']
        
        # Check minimum value if specified
        min_check = True
        if self.min_value is not None:
            min_check = min_val >= self.min_value
        
        # Check maximum value if specified
        max_check = True
        if self.max_value is not None:
            max_check = max_val <= self.max_value
        
        passed = min_check and max_check
        
        # Create appropriate message
        if self.min_value is not None and self.max_value is not None:
            message = f"Values range from {min_val} to {max_val} (expected: {self.min_value} to {self.max_value})"
        elif self.min_value is not None:
            message = f"Minimum value is {min_val} (expected at least {self.min_value})"
        elif self.max_value is not None:
            message = f"Maximum value is {max_val} (expected at most {self.max_value})"
        else:
            message = f"Values range from {min_val} to {max_val}"
        
        return {
            "passed": passed,
            "actual_min": min_val,
            "actual_max": max_val,
            "expected_min": self.min_value,
            "expected_max": self.max_value,
            "message": message
        }
    
class RuleEngine:
    """
    Engine for managing and evaluating data quality issues.
    """

    def __init__(self):
        """Initialise an empty rule engine."""
        self.rules = []
        logger.info('Initialised RuleEngine')

    def add_rule(self, rule):
        """
        Add a rule to the engine.

        Args:
            rule: Rule instance to add
        """
        self.rules.append(rule)
        logger.info(f"Added rule: {rule.name} - {rule.description}")
    
    def evaluate_column(self, column_name, column_stats):
        """
        Evaluate all rules for a specific column.

        Args:
            column_name: Name of the column
            column_stats: Dictionary of statistics for the column

        Returns:
            list: List of evaluation results for each rule
        """
        logger.info(f"Evaluating {len(self.rules)} rules for column: {column_name}")
        results = []
        
        for rule in self.rules:
            if not rule.is_applicable(column_stats):
                logger.info(f"Skipping rule {rule.name} - not applicable to column {column_name} of type {column_stats.get('type', 'unknown')}")
                continue

            try:
                result = rule.evaluate(column_stats)
                results.append({
                    "rule_name": rule.name,
                    "description": rule.description,
                    "result": result
                })
                
                status = "PASSED" if result.get("passed", False) else "FAILED"
                logger.debug(f"Rule {rule.name} {status} for column {column_name}")
                
            except Exception as e:
                logger.error(f"Error evaluating rule {rule.name} for column {column_name}: {e}")
                results.append({
                    "rule_name": rule.name,
                    "description": rule.description,
                    "result": {
                        "passed": False,
                        "reason": "Evaluation error",
                        "details": str(e)
                    }
                })
        
        # Count passed and failed rules
        passed_count = sum(1 for r in results if r['result'].get('passed', False))
        failed_count = len(results) - passed_count
        
        logger.info(f"Column {column_name}: {passed_count} rules passed, {failed_count} rules failed")
        return results
    
    def evaluate_dataframe(self, analysis_results):
        """
        Evaluate all rules for all columns in the DataFrame/

        Args:
            analysis_results: Dictionary of column statistics from StatisticalAnalyser

        Returns:
            dict: Dictionary of evaluation results by column
        """
        logger.info(f"Evaluating rules for {len(analysis_results)} columns")
        results = {}
        
        for column_name, column_stats in analysis_results.items():
            results[column_name] = self.evaluate_column(column_name, column_stats)
        
        return results