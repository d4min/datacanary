# DataCanary Quick Start Guide

DataCanary is a data quality tool that helps you analyze and validate your data in AWS S3. This guide will help you get started quickly.

## Installation

### Prerequisites

- Python 3.7 or higher
- AWS credentials configured
- Access to S3 buckets containing Parquet files

### Install DataCanary

Install DataCanary using pip in development mode:

```bash
# From the project root directory
pip install -e .
```

## Basic Usage

DataCanary DataCanary provides two main commands:

### Analyze Data
To analyze a Parquet file stored in S3:

```bash
datacanary analyze --bucket your-bucket-name --key path/to/file.parquet
```

This will print statistics about each column in the dataset. To save the results to a file:

```bash
datacanary analyze --bucket your-bucket-name --key path/to/file.parquet --output analysis.json
```

### Check Data Quality
To run data quality checks on a parquet file

```bash
datacanary check --bucket your-bucket-name --key path/to/file.parquet
```

This will analyze the data and evaluate quality rules, printing a report. To save the report:

```bash
datacanary check --bucket your-bucket-name --key path/to/file.parquet --report report.txt --json results.json
```

## Getting Help
For more information, run:

```bash
datacanary --help
datacanary analyze --help
datacanary check --help
```
