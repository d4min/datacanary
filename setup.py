from setuptools import setup, find_packages

setup(
    name="datacanary",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "pandas",
        "boto3",
        "pyarrow",
    ],
    entry_points={
        "console_scripts": [
            "datacanary=datacanary.__main__:main",
        ],
    },
)