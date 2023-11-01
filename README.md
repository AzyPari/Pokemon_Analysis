# Pokemon Analysis with PySpark

Analyze Pokemon data using PySpark to extract insights like the top non-legendary Pokemon, the type with the highest average HP, and the most common special attack value.

## Table of Contents

Directory Structure
Requirements
Setup
Running the Analysis
Testing

## Directory Structure

pokemon-analysis/
│
├── data/
│   pokemon.csv
│
├── config/
│   settings.py
│
├── tests/
│   __init__.py
│   test_analysis.py
│
├── analysis.py
│
└── requirements.txt


## Requirements

Python 3.7+
PySpark
pytest

## Setup

Clone the repository:

	git clone [repository_url] pokemon-analysis
	
	cd pokemon-analysis
	
Create a virtual environment and activate it:

	python3 -m venv venv
	source venv/bin/activate
	
Install the required packages:

	pip install -r requirements.txt
	
## Running the Analysis

Once you have set everything up, you can run the analysis with:

	python analysis/main.py

## Testing

Tests have been written using the pytest framework. To run the tests:

Ensure you're in the project root directory.

	pytest tests/
