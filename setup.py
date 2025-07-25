# setup.py
from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="snowflake-migration-validator",
    version="1.0.0",
    author="Your Organization",
    author_email="data-team@yourorg.com",
    description="Production-ready Snowflake data migration validation tool",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourorg/snowflake-migration-validator",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Data Engineers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Database",
        "Topic :: Software Development :: Quality Assurance",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "snowflake-validator=snowflake_validator.main:main",
        ],
    },
    include_package_data=True,
    package_data={
        "snowflake_validator": [
            "templates/*.html",
            "templates/*.css",
            "config/*.yaml",
        ],
    },
)