"""
Logging configuration for DataCanary.
"""
import logging
import sys

def setup_logging(level=None):
    """
    Set up basic logging configuration.
    
    Args:
        level: Logging level (defaults to INFO if None)
    """
    if level is None:
        level = logging.INFO
        
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # Set boto3 and s3fs logging to WARNING to reduce noise
    logging.getLogger('boto3').setLevel(logging.WARNING)
    logging.getLogger('botocore').setLevel(logging.WARNING)
    
    logging.info(f"Logging configured with level {logging.getLevelName(level)}")