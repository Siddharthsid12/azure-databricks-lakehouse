"""Package setup for the Lakehouse pipeline."""

from setuptools import setup, find_packages

setup(
    name="azure-databricks-lakehouse",
    version="1.0.0",
    description="End-to-end data lakehouse on Azure Databricks with Medallion Architecture",
    author="Your Name",
    packages=find_packages(exclude=["tests*"]),
    python_requires=">=3.10",
    install_requires=[
        "pyspark>=3.5.0",
        "delta-spark>=3.1.0",
        "pyyaml>=6.0",
    ],
    extras_require={
        "dev": [
            "pytest>=8.0",
            "pytest-cov>=5.0",
            "ruff>=0.3.0",
            "black>=24.0",
        ],
    },
)
