"""
Rule configuration module for DataCanary.
Allows defining custom rules via configuration files.
"""
import os 
import json 
import yaml
import logging

from datacanary.rules.rule_engine import (
    Rule, NullPercentageRule, UniqueValueRule, ValueRangeRule, RuleEngine
)

logger = logging.getLogger(__name__)

def load_rules_from_file(file_path):
    """
    Load rule definitions from a YAML or JSON file.

    Args:
        file_path: Path to the rule configuration file

    Returns:
        List of Rule Instance
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Rule configuration file not found: {file_path}")
    
    # Determine file type based on extension
    file_ext = os.path.splitext(file_path)[1].lower()

    try:
        if file_ext == '.yaml' or file_ext == '.yml':
            with open(file_path, 'r') as f:
                config = yaml.safe_load(f)
        elif file_ext == '.json':
            with open(file_path, 'r') as f:
                config = json.load(f)
        else:
            raise ValueError(f"Unsupported file extension: {file_ext}. Use .yaml, .yml, or .json")
    
        if not isinstance(config, dict) or 'rules' not in config:
                raise ValueError("Invalid configuration format: missing 'rules' key")
            
        rules = []
        for rule_config in config['rules']:
            rule = create_rule_from_config(rule_config)
            if rule:
                rules.append(rule)
            
        logger.info(f"Loaded {len(rules)} rules from {file_path}")
        return rules
        
    except Exception as e:
        logger.error(f"Error loading rules from {file_path}: {e}")
        raise

def create_rule_from_config(rule_config):
    """
    Create a Rule Instance from a config dictionary.

    Args:
        rule_config: Dictionary containing rule configuration
    
    Returns:
        Rule instance or None is config is invalid
    """
    if 'type' not in rule_config:
        logger.warning("Rule configuration missing 'type' key, skipping")
        return None
    
    rule_type = rule_config['type']

    try:
        if rule_type == 'null_percentage':
            threshold = rule_config.get('threshold', 5.0)
            return NullPercentageRule(threshold=threshold)
        
        elif rule_type == 'unique_value':
            threshold = rule_config.get('threshold', 90.0)
            return UniqueValueRule(threshold=threshold)
        
        elif rule_type == 'value_range':
            min_value = rule_config.get('min_value')
            max_value = rule_config.get('max_value')
            return ValueRangeRule(min_value=min_value, max_value=max_value)
        
        else:
            logger.warning(f"Unknown rule type: {rule_type}")
            return None
    
    except Exception as e:
        logger.error(f"Error creating rule of type {rule_type}: {e}")
        return None
    
def apply_rules_to_engine(rule_engine, file_path):
    """
    Load rules from a file and add them to a rule engine. 

    Args:
        rule_engine: RuleEngine instance to add rules to 
        file_path: Path to the rule configuration file 
    """
    rules = load_rules_from_file(file_path)
    for rule in rules:
        rule_engine.add_rule(rule)


