"""
Tests for the StatisticalAnalyser class.
"""
import unittest
import pandas as pd
import numpy as np

from datacanary.analysers.statistical_analyser import StatisticalAnalyser

class TestStatisticalAnalyser(unittest.TestCase):
    """Test cases for the StatisticalAnalyser class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.analyser = StatisticalAnalyser()
        
        # Create a test DataFrame with various column types
        self.test_df = pd.DataFrame({
            'numeric_col': [1, 2, 3, 4, 5, None, 0, -1],
            'string_col': ['a', 'b', 'c', 'a', '', None, 'xyz', 'abc'],
            'date_col': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03', 
                                       '2023-01-01', '2023-02-01', None, '2023-03-01', '2023-04-01']),
            'all_null_col': [None, None, None, None, None, None, None, None]
        })
    
    def test_analyse_dataframe(self):
        """Test that the analyser can process a DataFrame."""
        results = self.analyser.analyse_dataframe(self.test_df)
        
        # Check that all columns were analysed
        self.assertEqual(len(results), 4)
        self.assertIn('numeric_col', results)
        self.assertIn('string_col', results)
        self.assertIn('date_col', results)
        self.assertIn('all_null_col', results)
        
        # Check numeric column stats
        numeric_stats = results['numeric_col']['stats']
        self.assertEqual(numeric_stats['count'], 8)
        self.assertEqual(numeric_stats['null_count'], 1)
        self.assertEqual(numeric_stats['min'], -1)
        self.assertEqual(numeric_stats['max'], 5)
        self.assertEqual(numeric_stats['zeros_count'], 1)
        
        # Check string column stats
        string_stats = results['string_col']['stats']
        self.assertEqual(string_stats['null_count'], 1)
        self.assertEqual(string_stats['unique_count'], 6)
        self.assertEqual(string_stats['empty_string_count'], 1)
        
        # Check date column stats
        date_stats = results['date_col']['stats']
        self.assertEqual(date_stats['null_count'], 1)
        self.assertEqual(date_stats['unique_count'], 6)
        
        # Check all-null column
        null_stats = results['all_null_col']['stats']
        self.assertEqual(null_stats['null_count'], 8)
        self.assertEqual(null_stats['null_percentage'], 100)
    
    def test_invalid_input(self):
        """Test that the analyser rejects non-DataFrame inputs."""
        with self.assertRaises(TypeError):
            self.analyser.analyse_dataframe("not a dataframe")
    
    def test_empty_dataframe(self):
        """Test that the analyser handles empty DataFrames gracefully."""
        empty_df = pd.DataFrame()
        results = self.analyser.analyse_dataframe(empty_df)
        self.assertEqual(results, {})

if __name__ == '__main__':
    unittest.main()