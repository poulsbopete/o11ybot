# Elastic Observability Analyzer

This application connects to an Elastic Serverless observability instance and analyzes APM trace data to provide valuable insights and ESQL dashboard examples.

## Features

- Connects to Elastic Serverless observability
- Analyzes APM indices for valuable metrics
- Extracts purchase amounts, LCP (Largest Contentful Paint), geo location data
- Generates ESQL examples for creating compelling dashboards

## Setup

1. Create a `.env` file with your Elastic credentials:
```
ELASTIC_URL=your_elastic_url
ELASTIC_API_KEY=your_api_key
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Run the application:
```bash
python main.py
```

## Output

The application will:
- Analyze your APM indices
- Identify valuable metrics and patterns
- Generate ESQL examples for creating dashboards
- Provide recommendations for visualization

## Requirements

- Python 3.8+
- Elastic Serverless observability instance
- Valid Elastic API key with appropriate permissions 