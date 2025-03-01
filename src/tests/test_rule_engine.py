"""
Tests for the rule engine and rules.
"""
import unittest
from datacanary.rules.rule_engine import Rule, NullPercentageRule, UniqueValueRule, ValueRangeRule, RuleEngine

class TestRules(unittest.TestCase):
    """Test cases for the individual rule classes."""
    
    def test_null_percentage_rule(self):
        """Test the NullPercentageRule."""
        rule = NullPercentageRule(threshold=10.0)
        
        # Test when nulls are below threshold
        stats = {'stats': {'null_percentage': 5.0}}
        result = rule.evaluate(stats)
        self.assertTrue(result['passed'])
        
        # Test when nulls are above threshold
        stats = {'stats': {'null_percentage': 15.0}}
        result = rule.evaluate(stats)
        self.assertFalse(result['passed'])
        
        # Test with missing statistics
        stats = {'stats': {}}
        result = rule.evaluate(stats)
        self.assertFalse(result['passed'])
    
    def test_unique_value_rule(self):
        """Test the UniqueValueRule."""
        rule = UniqueValueRule(threshold=50.0)
        
        # Test when unique values are above threshold
        stats = {'stats': {'unique_percentage': 75.0}}
        result = rule.evaluate(stats)
        self.assertTrue(result['passed'])
        
        # Test when unique values are below threshold
        stats = {'stats': {'unique_percentage': 25.0}}
        result = rule.evaluate(stats)
        self.assertFalse(result['passed'])
        
        # Test with missing statistics
        stats = {'stats': {}}
        result = rule.evaluate(stats)
        self.assertFalse(result['passed'])
    
    def test_value_range_rule(self):
        """Test the ValueRangeRule."""
        # Test with min and max
        rule = ValueRangeRule(min_value=0, max_value=100)
        
        # Test when values are within range
        stats = {'type': 'int64', 'stats': {'min': 10, 'max': 90}}
        result = rule.evaluate(stats)
        self.assertTrue(result['passed'])
        
        # Test when values are outside range
        stats = {'type': 'int64', 'stats': {'min': -10, 'max': 110}}
        result = rule.evaluate(stats)
        self.assertFalse(result['passed'])
        
        # Test with only min
        rule = ValueRangeRule(min_value=0)
        stats = {'type': 'int64', 'stats': {'min': 10, 'max': 90}}
        result = rule.evaluate(stats)
        self.assertTrue(result['passed'])
        
        # Test with only max
        rule = ValueRangeRule(max_value=100)
        stats = {'type': 'int64', 'stats': {'min': 10, 'max': 90}}
        result = rule.evaluate(stats)
        self.assertTrue(result['passed'])
        
        # Test non-numeric column
        rule = ValueRangeRule(min_value=0, max_value=100)
        stats = {'type': 'object', 'stats': {'min': 0, 'max': 10}}
        result = rule.evaluate(stats)
        self.assertFalse(result['passed'])
        self.assertEqual(result['reason'], "Not applicable")

class TestRuleEngine(unittest.TestCase):
    """Test cases for the RuleEngine class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.engine = RuleEngine()
        self.engine.add_rule(NullPercentageRule(threshold=5.0))
        self.engine.add_rule(UniqueValueRule(threshold=90.0))
        
        # Sample analysis results
        self.analysis_results = {
            'id_column': {
                'type': 'int64',
                'stats': {
                    'null_percentage': 0.0,
                    'unique_percentage': 100.0,
                    'min': 1,
                    'max': 100
                }
            },
            'value_column': {
                'type': 'float64',
                'stats': {
                    'null_percentage': 10.0,
                    'unique_percentage': 50.0,
                    'min': 0,
                    'max': 1000
                }
            }
        }
    
    def test_add_rule(self):
        """Test adding rules to the engine."""
        engine = RuleEngine()
        self.assertEqual(len(engine.rules), 0)
        
        engine.add_rule(NullPercentageRule())
        self.assertEqual(len(engine.rules), 1)
        
        engine.add_rule(UniqueValueRule())
        self.assertEqual(len(engine.rules), 2)
    
    def test_evaluate_column(self):
        """Test evaluating rules for a single column."""
        # Test column that passes all rules
        results = self.engine.evaluate_column('id_column', self.analysis_results['id_column'])
        self.assertEqual(len(results), 2)
        self.assertTrue(results[0]['result']['passed'])
        self.assertTrue(results[1]['result']['passed'])
        
        # Test column that fails some rules
        results = self.engine.evaluate_column('value_column', self.analysis_results['value_column'])
        self.assertEqual(len(results), 2)
        self.assertFalse(results[0]['result']['passed'])  # Fails null percentage
        self.assertFalse(results[1]['result']['passed'])  # Fails unique percentage
    
    def test_evaluate_dataframe(self):
        """Test evaluating rules for all columns in a DataFrame."""
        results = self.engine.evaluate_dataframe(self.analysis_results)
        
        self.assertEqual(len(results), 2)
        self.assertIn('id_column', results)
        self.assertIn('value_column', results)
        
        # Check that id_column passed all rules
        id_results = results['id_column']
        self.assertEqual(len(id_results), 2)
        self.assertTrue(id_results[0]['result']['passed'])
        self.assertTrue(id_results[1]['result']['passed'])
        
        # Check that value_column failed some rules
        value_results = results['value_column']
        self.assertEqual(len(value_results), 2)
        self.assertFalse(value_results[0]['result']['passed'])
        self.assertFalse(value_results[1]['result']['passed'])

if __name__ == '__main__':
    unittest.main()