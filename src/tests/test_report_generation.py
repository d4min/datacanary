"""
Tests for the report generator.
"""
import unittest

from datacanary.reporting.report_generator import ReportGenerator

class TestReportGenerator(unittest.TestCase):
    """Test cases for the ReportGenerator class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.reporter = ReportGenerator()
        
        # Sample analysis results
        self.analysis_results = {
            'test_column': {
                'type': 'int64',
                'stats': {
                    'count': 100,
                    'null_count': 0,
                    'null_percentage': 0.0,
                    'unique_count': 100,
                    'min': 1,
                    'max': 100
                }
            }
        }
        
        # Sample rule results
        self.rule_results = {
            'test_column': [
                {
                    'rule_name': 'test_rule',
                    'description': 'Test rule description',
                    'result': {
                        'passed': True,
                        'message': 'Test passed'
                    }
                }
            ]
        }
    
    def test_generate_text_report(self):
        """Test the text report generation."""
        report = self.reporter.generate_text_report(
            'test_dataset',
            self.analysis_results,
            self.rule_results
        )
        
        # Check that the report is a non-empty string
        self.assertIsInstance(report, str)
        self.assertTrue(len(report) > 0)
        
        # Check that the report contains expected elements
        self.assertIn('DataCanary Quality Report', report)
        self.assertIn('test_dataset', report)
        self.assertIn('test_column', report)
        self.assertIn('test_rule', report)
        self.assertIn('100', report)  # Should indicate 100% pass rate
        self.assertIn('PASSED', report)  # Overall status

if __name__ == '__main__':
    unittest.main()