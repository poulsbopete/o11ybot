# Elastic Observability Analyzer

This application connects to an Elastic Serverless observability instance and analyzes APM trace data to provide valuable insights and ESQL dashboard examples.

## Features

- Connects to Elastic Serverless observability
- Analyzes APM indices for valuable metrics
- Extracts purchase amounts, LCP (Largest Contentful Paint), geo location data
- Generates ESQL examples for creating compelling dashboards

## Setup

### 1. Create and Activate a Python Virtual Environment (macOS)

1. **Install Python 3.11 (if not already installed):**
   ```bash
   brew install python@3.11
   ```
2. **Create a virtual environment:**
   ```bash
   /opt/homebrew/opt/python@3.11/bin/python3.11 -m venv venv
   ```
3. **Activate the virtual environment:**
   ```bash
   source venv/bin/activate
   ```
4. **Upgrade pip (recommended):**
   ```bash
   pip install --upgrade pip
   ```

### 2. Set up your Elastic credentials

Create a `.env` file with your Elastic credentials:
```
ELASTIC_URL=your_elastic_url
ELASTIC_API_KEY=your_api_key
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Run the application
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

- Python 3.11+
- Elastic Serverless observability instance
- Valid Elastic API key with appropriate permissions 