import os
from elasticsearch import Elasticsearch
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table
import json
import requests
from urllib.parse import urljoin

# Load environment variables
load_dotenv()

# Initialize Rich console for pretty output
console = Console()

class ElasticAnalyzer:
    def __init__(self):
        self.base_url = os.getenv('ELASTIC_URL')
        self.api_key = os.getenv('ELASTIC_API_KEY')
        
        # Initialize Elasticsearch client with proper configuration
        self.es = Elasticsearch(
            self.base_url,
            api_key=self.api_key,
            verify_certs=True,
            timeout=30,
            max_retries=3,
            retry_on_timeout=True
        )
        self.console = Console()

    def test_connection(self):
        """Test the connection to Elasticsearch"""
        try:
            # Test basic connectivity
            health = self.es.cluster.health()
            self.console.print(f"[green]Successfully connected to Elasticsearch cluster: {health['cluster_name']}[/green]")
            return True
        except Exception as e:
            self.console.print(f"[bold red]Error connecting to Elasticsearch: {str(e)}[/bold red]")
            return False

    def get_apm_indices(self):
        """Get all APM-related indices"""
        try:
            # First try to get indices using the standard pattern
            indices = self.es.indices.get_alias(index="apm-*")
            return list(indices.keys())
        except Exception as e:
            self.console.print(f"[yellow]Warning: Could not find APM indices using standard pattern: {str(e)}[/yellow]")
            try:
                # Try to get all indices and filter for APM-related ones
                all_indices = self.es.indices.get_alias(index="*")
                apm_indices = [idx for idx in all_indices.keys() if 'apm' in idx.lower() or 'otel' in idx.lower()]
                return apm_indices
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
        
        # Example 1: Purchase Analysis
        examples.append({
            "title": "Purchase Analysis",
            "description": "Analyze purchase amounts and success rates",
            "esql": f"""
            FROM {index}
            | WHERE transaction.type == "purchase"
            | STATS 
                avg_amount = AVG(transaction.amount),
                success_rate = AVG(transaction.success),
                count = COUNT()
            | SORT count DESC
            """
        })

        # Example 2: Performance Metrics
        examples.append({
            "title": "Performance Metrics",
            "description": "Analyze LCP and other performance metrics",
            "esql": f"""
            FROM {index}
            | WHERE transaction.type == "page-load"
            | STATS 
                avg_lcp = AVG(transaction.lcp),
                p95_lcp = PERCENTILE(transaction.lcp, 95),
                count = COUNT()
            | SORT avg_lcp DESC
            """
        })

        # Example 3: Geographic Analysis
        examples.append({
            "title": "Geographic Analysis",
            "description": "Analyze user locations and performance by region",
            "esql": f"""
            FROM {index}
            | WHERE transaction.type == "page-load"
            | STATS 
                avg_lcp = AVG(transaction.lcp),
                count = COUNT()
                BY geoip.country_name
            | SORT avg_lcp DESC
            """
        })

        # Example 4: OTEL-specific Analysis
        examples.append({
            "title": "OTEL Trace Analysis",
            "description": "Analyze OpenTelemetry trace data",
            "esql": f"""
            FROM {index}
            | WHERE span.kind == "server"
            | STATS 
                avg_duration = AVG(span.duration.us),
                p95_duration = PERCENTILE(span.duration.us, 95),
                count = COUNT()
                BY service.name
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
            self.console.print(f"[bold red]Error during analysis: {str(e)}[/bold red]")

def main():
    analyzer = ElasticAnalyzer()
    analyzer.run_analysis()

if __name__ == "__main__":
    main() 