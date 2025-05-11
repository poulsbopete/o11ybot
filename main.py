import os
from elasticsearch import Elasticsearch
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table
import json
import requests
from urllib.parse import urljoin

# Force reload environment variables
load_dotenv(override=True)

# Initialize Rich console for pretty output
console = Console()

# Set the correct values directly
os.environ['ELASTIC_URL'] = 'https://otel-demo-a5630c.es.us-east-1.aws.elastic.cloud'
os.environ['ELASTIC_API_KEY'] = 'ApiKey UHd2cXdKWUJ1aV9MRG9GdzAya206MFcwNVRfbkVhUjhIZ05hTEhtNnlzUQ=='

# Debug: Print environment variables
console.print("[yellow]Environment Variables:[/yellow]")
console.print(f"ELASTIC_URL: {os.getenv('ELASTIC_URL')}")
console.print(f"ELASTIC_API_KEY: {os.getenv('ELASTIC_API_KEY')[:20]}...")  # Only show first 20 chars for security

class ElasticAnalyzer:
    def __init__(self):
        self.base_url = os.getenv('ELASTIC_URL')
        self.api_key = os.getenv('ELASTIC_API_KEY')
        
        if not self.base_url or not self.api_key:
            raise ValueError("ELASTIC_URL and ELASTIC_API_KEY must be set in .env file")
        
        console.print(f"[yellow]Attempting to connect to: {self.base_url}[/yellow]")
        
        # Initialize Elasticsearch client with proper configuration
        self.es = Elasticsearch(
            self.base_url,
            headers={"Authorization": self.api_key},
            verify_certs=True,
            request_timeout=30,
            max_retries=3,
            retry_on_timeout=True
        )
        self.console = Console()

    def test_connection(self):
        """Test the connection to Elasticsearch"""
        try:
            # Try a simple search query to test connection
            response = requests.post(
                f"{self.base_url}/logs-*/_search",
                headers={
                    "Authorization": self.api_key,
                    "Content-Type": "application/json"
                },
                json={
                    "size": 0,
                    "query": {
                        "match_all": {}
                    }
                },
                verify=True
            )
            self.console.print(f"[yellow]Direct HTTP response status: {response.status_code}[/yellow]")
            if response.status_code != 200:
                # Escape any special characters in the response text
                error_text = response.text.replace('[', '\\[').replace(']', '\\]')
                self.console.print(f"[yellow]Response content: {error_text}[/yellow]")
                return False
            
            # If we get here, the connection is working
            self.console.print("[green]Successfully connected to Elasticsearch serverless instance[/green]")
            return True
        except Exception as e:
            error_msg = str(e).replace('[', '\\[').replace(']', '\\]')
            self.console.print(f"[bold red]Error connecting to Elasticsearch: {error_msg}[/bold red]")
            self.console.print(f"[yellow]Please verify your ELASTIC_URL ({self.base_url}) and ELASTIC_API_KEY are correct[/yellow]")
            return False

    def get_apm_indices(self):
        """Get all APM-related indices"""
        try:
            # Try to search in common APM index patterns
            apm_patterns = [
                "apm-*",
                "traces-*",
                "logs-*",
                "metrics-*"
            ]
            
            found_indices = set()
            for pattern in apm_patterns:
                response = requests.post(
                    f"{self.base_url}/{pattern}/_search",
                    headers={
                        "Authorization": self.api_key,
                        "Content-Type": "application/json"
                    },
                    json={
                        "size": 0,
                        "query": {
                            "match_all": {}
                        }
                    },
                    verify=True
                )
                if response.status_code == 200:
                    # Extract index name from response
                    index_name = response.url.split('/')[-2]  # Get index name from URL
                    found_indices.add(index_name)
            
            return list(found_indices)
        except Exception as e:
            self.console.print(f"[bold red]Error getting indices: {str(e)}[/bold red]")
            return []

    def analyze_trace_data(self, index):
        """Analyze trace data in a specific index"""
        try:
            # Search for transaction events
            query = {
                "size": 0,
                "aggs": {
                    "transaction_types": {
                        "terms": {
                            "field": "transaction.type",
                            "size": 10
                        }
                    }
                }
            }
            
            response = self.es.search(index=index, body=query)
            return response['aggregations']['transaction_types']['buckets']
        except Exception as e:
            self.console.print(f"[yellow]Warning: Could not analyze trace data for index {index}: {str(e)}[/yellow]")
            return []

    def generate_esql_examples(self, index):
        """Generate ESQL examples based on the data found"""
        examples = []
        
        # Example 1: Transaction Duration Analysis
        examples.append({
            "title": "Transaction Duration Analysis",
            "description": "Analyze transaction durations by type",
            "esql": f"""
            FROM {index}
            | WHERE transaction.type IS NOT NULL
            | STATS 
                avg_duration = AVG(transaction.duration.us),
                p95_duration = PERCENTILE(transaction.duration.us, 95),
                count = COUNT()
                BY transaction.type
            | SORT avg_duration DESC
            """
        })

        # Example 2: Error Analysis
        examples.append({
            "title": "Error Analysis",
            "description": "Analyze errors by transaction type",
            "esql": f"""
            FROM {index}
            | WHERE transaction.result == "error"
            | STATS 
                error_count = COUNT(),
                avg_duration = AVG(transaction.duration.us)
                BY transaction.type
            | SORT error_count DESC
            """
        })

        # Example 3: Service Performance
        examples.append({
            "title": "Service Performance",
            "description": "Analyze performance by service",
            "esql": f"""
            FROM {index}
            | WHERE service.name IS NOT NULL
            | STATS 
                avg_duration = AVG(transaction.duration.us),
                p95_duration = PERCENTILE(transaction.duration.us, 95),
                count = COUNT()
                BY service.name
            | SORT avg_duration DESC
            """
        })

        # Example 4: Span Analysis
        examples.append({
            "title": "Span Analysis",
            "description": "Analyze spans by type and service",
            "esql": f"""
            FROM {index}
            | WHERE span.type IS NOT NULL
            | STATS 
                avg_duration = AVG(span.duration.us),
                count = COUNT()
                BY span.type, service.name
            | SORT avg_duration DESC
            """
        })

        # Example 5: Geographic Distribution
        examples.append({
            "title": "Geographic Distribution",
            "description": "Analyze transactions by geographic location",
            "esql": f"""
            FROM {index}
            | WHERE client.geo.country_name IS NOT NULL
            | STATS 
                avg_duration = AVG(transaction.duration.us),
                count = COUNT()
                BY client.geo.country_name
            | SORT count DESC
            """
        })

        # Example 6: User Experience Metrics
        examples.append({
            "title": "User Experience Metrics",
            "description": "Analyze user experience metrics",
            "esql": f"""
            FROM {index}
            | WHERE transaction.type == "page-load"
            | STATS 
                avg_duration = AVG(transaction.duration.us),
                p95_duration = PERCENTILE(transaction.duration.us, 95),
                count = COUNT()
                BY transaction.name
            | SORT avg_duration DESC
            """
        })

        return examples

    def run_analysis(self):
        """Run the complete analysis"""
        try:
            # Test connection first
            if not self.test_connection():
                return

            # Get APM indices
            indices = self.get_apm_indices()
            if not indices:
                self.console.print("[yellow]No APM indices found. Make sure your OTEL data is being properly ingested.[/yellow]")
                return

            self.console.print(f"\n[bold green]Found {len(indices)} APM indices[/bold green]")
            
            for index in indices:
                self.console.print(f"\n[bold blue]Analyzing index: {index}[/bold blue]")
                
                # Analyze trace data
                trace_data = self.analyze_trace_data(index)
                
                if trace_data:
                    # Create a table for transaction types
                    table = Table(title=f"Transaction Types in {index}")
                    table.add_column("Type", style="cyan")
                    table.add_column("Count", style="magenta")
                    
                    for bucket in trace_data:
                        table.add_row(bucket['key'], str(bucket['doc_count']))
                    
                    self.console.print(table)
                else:
                    self.console.print("[yellow]No transaction data found in this index[/yellow]")
                
                # Generate and display ESQL examples
                examples = self.generate_esql_examples(index)
                
                for example in examples:
                    self.console.print(f"\n[bold yellow]{example['title']}[/bold yellow]")
                    self.console.print(f"[italic]{example['description']}[/italic]")
                    self.console.print("[green]ESQL Example:[/green]")
                    self.console.print(example['esql'])
                
        except Exception as e:
            error_msg = str(e).replace('[', '\\[').replace(']', '\\]')
            self.console.print(f"[bold red]Error during analysis: {error_msg}[/bold red]")

def main():
    analyzer = ElasticAnalyzer()
    analyzer.run_analysis()

if __name__ == "__main__":
    main() 