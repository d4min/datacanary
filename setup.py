from setuptools import setup, find_packages

setup(
    name="datacanary",
    version="0.1.0",
    description="Data quality monitoring tool",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "pandas",
        "boto3",
        "pyarrow",
    ],
    python_requires=">=3.8",
)