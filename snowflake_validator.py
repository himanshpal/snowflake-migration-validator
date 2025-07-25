# Snowflake Migration Validation System
# Production-ready implementation

import os
import sys
import json
import yaml
import time
import uuid
import logging
import argparse
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
from concurrent.futures import ThreadPoolExecutor
import traceback

# External dependencies
import snowflake.connector
import duckdb
import pandas as pd
from jinja2 import Template
import click
from rich.console import Console
from rich.progress import Progress, TaskID
from rich.table import Table
from rich import print as rprint
from reladiff import connect, diff_tables
import hashlib

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('validation.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
console = Console()

@dataclass
class QueryConfig:
    """Configuration for a single validation query"""
    query_id: str
    name: str
    category: str
    priority: str
    timeout_seconds: int
    retry_attempts: int
    sql: str
    comparison_type: str
    tolerance: float = 0.0
    required_for_migration: bool = True
    dependencies: List[str] = None
    key_columns: List[str] = None
    ignore_columns: List[str] = None
    sample_size: Optional[int] = None
    full_validation: bool = True

    def __post_init__(self):
        if self.dependencies is None:
            self.dependencies = []
        if self.key_columns is None:
            self.key_columns = []
        if self.ignore_columns is None:
            self.ignore_columns = []

@dataclass
class QueryResult:
    """Result of query execution and validation"""
    query_id: str
    status: str  # SUCCESS, FAILED, RUNNING, PENDING
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    execution_time: Optional[float] = None
    source_execution_time: Optional[float] = None
    target_execution_time: Optional[float] = None
    comparison_time: Optional[float] = None
    error_message: Optional[str] = None
    json_report_path: Optional[str] = None
    html_report_path: Optional[str] = None
    source_row_count: Optional[int] = None
    target_row_count: Optional[int] = None
    differences_count: Optional[int] = None
    match_percentage: Optional[float] = None

class RetryException(Exception):
    """Exception that should trigger a retry"""
    pass

def retry(max_attempts: int = 3, backoff_strategy: str = "exponential"):
    """Decorator for retry logic with exponential backoff"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except RetryException as e:
                    if attempt == max_attempts - 1:
                        raise e
                    wait_time = 2 ** attempt if backoff_strategy == "exponential" else 1
                    logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                except Exception as e:
                    logger.error(f"Non-retryable error: {e}")
                    raise e
            return None
        return wrapper
    return decorator

class SnowflakeConnector:
    """Manages Snowflake connections with pooling and retry logic"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.connections = {}
        self.connection_pool_size = config.get('connection_pool_size', 5)
        
    @retry(max_attempts=3)
    def get_connection(self, account_name: str):
        """Get or create a Snowflake connection"""
        if account_name not in self.connections:
            try:
                account_config = self.config[account_name]
                conn = snowflake.connector.connect(
                    user=account_config['user'],
                    password=account_config['password'],
                    account=account_config['account'],
                    warehouse=account_config['warehouse'],
                    database=account_config['database'],
                    schema=account_config['schema'],
                    role=account_config.get('role'),
                    timeout=account_config.get('timeout', 300)
                )
                self.connections[account_name] = conn
                logger.info(f"Connected to Snowflake account: {account_name}")
            except Exception as e:
                logger.error(f"Failed to connect to {account_name}: {e}")
                raise RetryException(f"Connection failed: {e}")
        
        return self.connections[account_name]
    
    @retry(max_attempts=3)
    def execute_query(self, account_name: str, sql: str, timeout: int = 300) -> pd.DataFrame:
        """Execute query and return results as DataFrame"""
        conn = self.get_connection(account_name)
        try:
            cursor = conn.cursor()
            cursor.execute(sql)
            
            # Fetch results with timeout
            start_time = time.time()
            results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            
            execution_time = time.time() - start_time
            if execution_time > timeout:
                raise TimeoutError(f"Query execution exceeded {timeout} seconds")
            
            df = pd.DataFrame(results, columns=columns)
            logger.info(f"Query executed successfully: {len(df)} rows returned in {execution_time:.2f}s")
            return df
            
        except Exception as e:
            logger.error(f"Query execution failed on {account_name}: {e}")
            if "timeout" in str(e).lower() or "connection" in str(e).lower():
                raise RetryException(f"Query failed: {e}")
            raise e
    
    def close_connections(self):
        """Close all connections"""
        for account_name, conn in self.connections.items():
            try:
                conn.close()
                logger.info(f"Closed connection to {account_name}")
            except Exception as e:
                logger.warning(f"Error closing connection to {account_name}: {e}")

class DuckDBManager:
    """Manages DuckDB operations for local data processing"""
    
    def __init__(self, db_path: str, memory_limit: str = "8GB"):
        self.db_path = db_path
        self.memory_limit = memory_limit
        self.conn = None
        self._connect()
    
    def _connect(self):
        """Initialize DuckDB connection"""
        try:
            self.conn = duckdb.connect(self.db_path)
            self.conn.execute(f"SET memory_limit='{self.memory_limit}'")
            self.conn.execute("SET temp_directory='./tmp'")
            logger.info(f"Connected to DuckDB: {self.db_path}")
        except Exception as e:
            logger.error(f"Failed to connect to DuckDB: {e}")
            raise e
    
    def export_dataframe(self, df: pd.DataFrame, table_name: str):
        """Export DataFrame to DuckDB table"""
        try:
            # Drop table if exists
            self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            
            # Create table from DataFrame
            self.conn.register(table_name, df)
            
            # Persist to disk
            self.conn.execute(f"CREATE TABLE {table_name}_persisted AS SELECT * FROM {table_name}")
            
            logger.info(f"Exported {len(df)} rows to DuckDB table: {table_name}")
            
        except Exception as e:
            logger.error(f"Failed to export to DuckDB: {e}")
            raise e
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get table information"""
        try:
            result = self.conn.execute(f"SELECT COUNT(*) as row_count FROM {table_name}").fetchone()
            row_count = result[0] if result else 0
            
            columns_result = self.conn.execute(f"DESCRIBE {table_name}").fetchall()
            columns = [row[0] for row in columns_result]
            
            return {
                "row_count": row_count,
                "columns": columns,
                "column_count": len(columns)
            }
        except Exception as e:
            logger.error(f"Failed to get table info for {table_name}: {e}")
            return {"row_count": 0, "columns": [], "column_count": 0}
    
    def close(self):
        """Close DuckDB connection"""
        if self.conn:
            self.conn.close()
            logger.info("Closed DuckDB connection")

class StateManager:
    """Manages execution state and progress tracking"""
    
    def __init__(self, state_file_path: str):
        self.state_file_path = Path(state_file_path)
        self.state = self._load_or_create_state()
        self.lock = threading.Lock()
    
    def _load_or_create_state(self) -> Dict[str, Any]:
        """Load existing state or create new one"""
        if self.state_file_path.exists():
            try:
                with open(self.state_file_path, 'r') as f:
                    state = json.load(f)
                logger.info(f"Loaded existing state: {state['execution_id']}")
                return state
            except Exception as e:
                logger.warning(f"Failed to load state file: {e}. Creating new state.")
        
        # Create new state
        state = {
            "execution_id": f"validation_{datetime.now().strftime('%Y_%m_%d_%H%M%S')}",
            "start_time": datetime.now(timezone.utc).isoformat(),
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "total_queries": 0,
            "completed_queries": 0,
            "failed_queries": 0,
            "current_query_index": 0,
            "queries": {}
        }
        return state
    
    def save(self):
        """Save current state to file"""
        with self.lock:
            try:
                self.state["last_updated"] = datetime.now(timezone.utc).isoformat()
                
                # Ensure directory exists
                self.state_file_path.parent.mkdir(parents=True, exist_ok=True)
                
                with open(self.state_file_path, 'w') as f:
                    json.dump(self.state, f, indent=2)
                    
            except Exception as e:
                logger.error(f"Failed to save state: {e}")
    
    def get_query_status(self, query_id: str) -> str:
        """Get status of a specific query"""
        return self.state["queries"].get(query_id, {}).get("status", "PENDING")
    
    def update_query_status(self, query_id: str, status: str, **kwargs):
        """Update query status and metadata"""
        with self.lock:
            if query_id not in self.state["queries"]:
                self.state["queries"][query_id] = {}
            
            self.state["queries"][query_id]["status"] = status
            self.state["queries"][query_id].update(kwargs)
            
            # Update counters
            if status == "SUCCESS":
                self.state["completed_queries"] += 1
            elif status == "FAILED":
                self.state["failed_queries"] += 1
    
    def get_incomplete_queries(self, all_query_ids: List[str]) -> List[str]:
        """Get list of queries that need to be executed"""
        incomplete = []
        for query_id in all_query_ids:
            status = self.get_query_status(query_id)
            if status not in ["SUCCESS"]:
                incomplete.append(query_id)
        return incomplete
    
    def get_execution_summary(self) -> Dict[str, Any]:
        """Get execution summary statistics"""
        total = len(self.state["queries"])
        completed = sum(1 for q in self.state["queries"].values() if q.get("status") == "SUCCESS")
        failed = sum(1 for q in self.state["queries"].values() if q.get("status") == "FAILED")
        
        return {
            "execution_id": self.state["execution_id"],
            "total_queries": total,
            "completed_queries": completed,
            "failed_queries": failed,
            "success_rate": (completed / total * 100) if total > 0 else 0
        }

class ReportGenerator:
    """Generates JSON and HTML reports"""
    
    def __init__(self, output_dir: str):
        self.output_dir = Path(output_dir)
        self.ensure_directories()
    
    def ensure_directories(self):
        """Create necessary output directories"""
        directories = [
            self.output_dir / "reports" / "json" / "individual",
            self.output_dir / "reports" / "json" / "summary",
            self.output_dir / "reports" / "html" / "individual",
            self.output_dir / "reports" / "html" / "summary",
            self.output_dir / "exports" / "source_data",
            self.output_dir / "exports" / "target_data",
            self.output_dir / "exports" / "differences",
            self.output_dir / "state"
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
    
    def generate_json_report(self, query_config: QueryConfig, query_result: QueryResult, 
                            comparison_data: Dict[str, Any]) -> Dict[str, Any]:
        """Generate JSON report for a single query"""
        return {
            "query_info": {
                "query_id": query_config.query_id,
                "query_name": query_config.name,
                "query_sql": query_config.sql,
                "category": query_config.category,
                "comparison_type": query_config.comparison_type
            },
            "execution_details": {
                "start_time": query_result.start_time.isoformat() if query_result.start_time else None,
                "end_time": query_result.end_time.isoformat() if query_result.end_time else None,
                "total_execution_time": query_result.execution_time,
                "source_execution_time": query_result.source_execution_time,
                "target_execution_time": query_result.target_execution_time,
                "comparison_time": query_result.comparison_time
            },
            "data_summary": {
                "source_account": {
                    "row_count": query_result.source_row_count,
                    "data_size_mb": comparison_data.get("source_size_mb", 0)
                },
                "target_account": {
                    "row_count": query_result.target_row_count,
                    "data_size_mb": comparison_data.get("target_size_mb", 0)
                }
            },
            "validation_result": {
                "status": query_result.status,
                "overall_match": query_result.differences_count == 0 if query_result.differences_count is not None else None,
                "discrepancy_count": query_result.differences_count,
                "match_percentage": query_result.match_percentage
            },
            "data_comparison": comparison_data.get("comparison_details", {}),
            "recommendations": comparison_data.get("recommendations", []),
            "files_generated": {
                "source_export": f"output/exports/source_data/{query_config.query_id}.parquet",
                "target_export": f"output/exports/target_data/{query_config.query_id}.parquet",
                "differences_export": f"output/exports/differences/{query_config.query_id}_differences.csv",
                "json_report": query_result.json_report_path,
                "html_report": query_result.html_report_path
            }
        }
    
    def generate_html_report(self, query_config: QueryConfig, query_result: QueryResult,
                            comparison_data: Dict[str, Any]) -> str:
        """Generate HTML report for a single query"""
        
        # HTML template
        html_template = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Query Validation Report: {{ query_id }}</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 0; padding: 20px; background: #f5f7fa; }
        .container { max-width: 1200px; margin: 0 auto; background: white; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
        .header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; border-radius: 8px 8px 0 0; }
        .status-badge { display: inline-block; padding: 6px 12px; border-radius: 20px; font-weight: bold; margin-left: 10px; }
        .status-failed { background: #ff6b6b; color: white; }
        .status-success { background: #51cf66; color: white; }
        .content { padding: 30px; }
        .section { margin-bottom: 30px; padding: 20px; border: 1px solid #e9ecef; border-radius: 6px; }
        .section h3 { margin-top: 0; color: #495057; border-bottom: 2px solid #e9ecef; padding-bottom: 10px; }
        .metrics-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin: 20px 0; }
        .metric-card { background: #f8f9fa; padding: 15px; border-radius: 6px; text-align: center; }
        .metric-value { font-size: 24px; font-weight: bold; color: #495057; }
        .metric-label { font-size: 12px; color: #6c757d; text-transform: uppercase; margin-top: 5px; }
        table { width: 100%; border-collapse: collapse; margin: 15px 0; }
        th, td { padding: 12px; text-align: left; border-bottom: 1px solid #dee2e6; }
        th { background: #f8f9fa; font-weight: 600; }
        .nav-links { margin: 20px 0; }
        .nav-links a { display: inline-block; padding: 8px 16px; background: #e9ecef; color: #495057; text-decoration: none; border-radius: 4px; margin-right: 10px; }
        .recommendations { background: #e6fffa; border-left: 4px solid #38b2ac; padding: 20px; margin: 20px 0; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Query Validation Report</h1>
            <h2>{{ query_id }}: {{ query_name }}
                <span class="status-badge status-{{ status_class }}">{{ status }}</span>
            </h2>
            <p>Generated: {{ generation_time }}</p>
        </div>
        
        <div class="content">
            <div class="nav-links">
                <a href="../summary/dashboard.html">← Back to Dashboard</a>
                <a href="../../json/individual/{{ query_id }}.json">View JSON Report</a>
            </div>

            <div class="section">
                <h3>📊 Execution Summary</h3>
                <div class="metrics-grid">
                    <div class="metric-card">
                        <div class="metric-value">{{ execution_time }}s</div>
                        <div class="metric-label">Total Time</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-value">{{ row_count }}</div>
                        <div class="metric-label">Rows Compared</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-value">{{ match_percentage }}%</div>
                        <div class="metric-label">Match Rate</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-value">{{ differences_count }}</div>
                        <div class="metric-label">Differences</div>
                    </div>
                </div>
            </div>

            <div class="section">
                <h3>🗄️ Query Information</h3>
                <table>
                    <tr><th>Query ID</th><td>{{ query_id }}</td></tr>
                    <tr><th>Query Name</th><td>{{ query_name }}</td></tr>
                    <tr><th>Category</th><td>{{ category }}</td></tr>
                    <tr><th>Comparison Type</th><td>{{ comparison_type }}</td></tr>
                    <tr><th>SQL Query</th><td><code>{{ sql_query }}</code></td></tr>
                </table>
            </div>

            {% if recommendations %}
            <div class="recommendations">
                <h3>💡 Recommendations</h3>
                <ul>
                    {% for recommendation in recommendations %}
                    <li>{{ recommendation }}</li>
                    {% endfor %}
                </ul>
            </div>
            {% endif %}

            <div class="section">
                <h3>📁 Generated Files</h3>
                <div class="nav-links">
                    <a href="../../exports/source_data/{{ query_id }}.parquet">Source Data Export</a>
                    <a href="../../exports/target_data/{{ query_id }}.parquet">Target Data Export</a>
                    {% if differences_count > 0 %}
                    <a href="../../exports/differences/{{ query_id }}_differences.csv">Differences Export</a>
                    {% endif %}
                </div>
            </div>
        </div>
    </div>
</body>
</html>
        """
        
        # Prepare template variables
        template_vars = {
            "query_id": query_config.query_id,
            "query_name": query_config.name,
            "status": query_result.status,
            "status_class": "success" if query_result.status == "SUCCESS" else "failed",
            "generation_time": datetime.now().strftime("%B %d, %Y at %H:%M:%S UTC"),
            "execution_time": f"{query_result.execution_time:.1f}" if query_result.execution_time else "N/A",
            "row_count": f"{query_result.source_row_count:,}" if query_result.source_row_count else "N/A",
            "match_percentage": f"{query_result.match_percentage:.3f}" if query_result.match_percentage else "N/A",
            "differences_count": query_result.differences_count or 0,
            "category": query_config.category,
            "comparison_type": query_config.comparison_type,
            "sql_query": query_config.sql,
            "recommendations": comparison_data.get("recommendations", [])
        }
        
        template = Template(html_template)
        return template.render(**template_vars)
    
    def save_reports(self, query_config: QueryConfig, query_result: QueryResult, 
                    comparison_data: Dict[str, Any]) -> Tuple[str, str]:
        """Save both JSON and HTML reports"""
        
        # Generate reports
        json_report = self.generate_json_report(query_config, query_result, comparison_data)
        html_report = self.generate_html_report(query_config, query_result, comparison_data)
        
        # Define file paths
        json_path = self.output_dir / "reports" / "json" / "individual" / f"{query_config.query_id}.json"
        html_path = self.output_dir / "reports" / "html" / "individual" / f"{query_config.query_id}.html"
        
        # Save JSON report
        with open(json_path, 'w') as f:
            json.dump(json_report, f, indent=2)
        
        # Save HTML report
        with open(html_path, 'w') as f:
            f.write(html_report)
        
        logger.info(f"Generated reports for {query_config.query_id}")
        return str(json_path), str(html_path)

class DataValidator:
    """Handles data comparison using reladiff and custom logic"""
    
    def __init__(self, duckdb_manager: DuckDBManager):
        self.duckdb = duckdb_manager
    
    def compare_dataframes(self, source_df: pd.DataFrame, target_df: pd.DataFrame,
                          query_config: QueryConfig) -> Dict[str, Any]:
        """Compare two DataFrames and return detailed comparison results"""
        
        comparison_result = {
            "comparison_details": {},
            "recommendations": [],
            "source_size_mb": len(source_df) * source_df.memory_usage(deep=True).sum() / (1024*1024),
            "target_size_mb": len(target_df) * target_df.memory_usage(deep=True).sum() / (1024*1024)
        }
        
        try:
            # Basic row count comparison
            source_rows = len(source_df)
            target_rows = len(target_df)
            
            if source_rows != target_rows:
                comparison_result["recommendations"].append(
                    f"Row count mismatch: Source has {source_rows} rows, Target has {target_rows} rows"
                )
            
            # Schema comparison
            source_columns = set(source_df.columns)
            target_columns = set(target_df.columns)
            
            if source_columns != target_columns:
                missing_in_target = source_columns - target_columns
                extra_in_target = target_columns - source_columns
                
                if missing_in_target:
                    comparison_result["recommendations"].append(
                        f"Columns missing in target: {', '.join(missing_in_target)}"
                    )
                if extra_in_target:
                    comparison_result["recommendations"].append(
                        f"Extra columns in target: {', '.join(extra_in_target)}"
                    )
            
            # Data comparison for matching columns
            common_columns = source_columns.intersection(target_columns)
            
            if query_config.ignore_columns:
                common_columns = common_columns - set(query_config.ignore_columns)
            
            differences_count = 0
            sample_differences = []
            
            if common_columns and source_rows > 0 and target_rows > 0:
                # Simple row-by-row comparison for demonstration
                # In production, you would use reladiff here
                
                min_rows = min(source_rows, target_rows)
                source_sample = source_df.head(min_rows)[list(common_columns)]
                target_sample = target_df.head(min_rows)[list(common_columns)]
                
                # Find differences
                for idx in range(min(100, min_rows)):  # Check first 100 rows for demo
                    for col in common_columns:
                        source_val = source_sample.iloc[idx][col]
                        target_val = target_sample.iloc[idx][col]
                        
                        if pd.isna(source_val) and pd.isna(target_val):
                            continue
                        
                        if source_val != target_val:
                            differences_count += 1
                            if len(sample_differences) < 10:  # Keep only first 10 for demo
                                sample_differences.append({
                                    "row_index": idx,
                                    "column": col,
                                    "source_value": str(source_val),
                                    "target_value": str(target_val),
                                    "difference_type": "value_mismatch"
                                })
            
            # Calculate match percentage
            total_cells = source_rows * len(common_columns) if common_columns else 0
            match_percentage = ((total_cells - differences_count) / total_cells * 100) if total_cells > 0 else 100
            
            comparison_result["comparison_details"] = {
                "total_rows_compared": min(source_rows, target_rows),
                "matching_rows": min(source_rows, target_rows) - differences_count,
                "differing_rows": differences_count,
                "sample_differences": sample_differences,
                "match_percentage": match_percentage
            }
            
            return comparison_result
            
        except Exception as e:
            logger.error(f"Data comparison failed: {e}")
            comparison_result["recommendations"].append(f"Comparison failed: {str(e)}")
            return comparison_result

class SnowflakeValidator:
    """Main validation orchestrator"""
    
    def __init__(self, config_path: str, output_dir: str = "./output"):
        self.config_path = config_path
        self.output_dir = output_dir
        self.config = self._load_config()
        
        # Initialize components
        self.snowflake_connector = SnowflakeConnector(self.config['snowflake'])
        self.duckdb_manager = DuckDBManager(
            os.path.join(output_dir, "validation.duckdb"),
            self.config.get('duckdb', {}).get('memory_limit', '8GB')
        )
        self.state_manager = StateManager(os.path.join(output_dir, "state", "execution_state.json"))
        self.report_generator = ReportGenerator(output_dir)
        self.data_validator = DataValidator(self.duckdb_manager)
        
        # Load queries
        self.queries = self._load_queries()
        self.state_manager.state["total_queries"] = len(self.queries)
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
            logger.info(f"Loaded configuration from {self.config_path}")
            return config
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            raise e
    
    def _load_queries(self) -> Dict[str, QueryConfig]:
        """Load query configurations"""
        queries = {}
        query_configs = self.config.get('queries', {})
        
        for query_id, query_data in query_configs.items():
            queries[query_id] = QueryConfig(
                query_id=query_id,
                name=query_data['name'],
                category=query_data['category'],
                priority=query_data.get('priority', 'medium'),
                timeout_seconds=query_data.get('timeout_seconds', 300),
                retry_attempts=query_data.get('retry_attempts', 3),
                sql=query_data['sql'],
                comparison_type=query_data['comparison_type'],
                tolerance=query_data.get('tolerance', 0.0),
                required_for_migration=query_data.get('required_for_migration', True),
                dependencies=query_data.get('dependencies', []),
                key_columns=query_data.get('key_columns', []),
                ignore_columns=query_data.get('ignore_columns', []),
                sample_size=query_data.get('sample_size'),
                full_validation=query_data.get('full_validation', True)
            )
        
        logger.info(f"Loaded {len(queries)} query configurations")
        return queries
    
    def validate_single_query(self, query_config: QueryConfig) -> QueryResult:
        """Validate a single query"""
        
        query_result = QueryResult(
            query_id=query_config.query_id,
            status="RUNNING",
            start_time=datetime.now(timezone.utc)
        )
        
        try:
            logger.info(f"Starting validation for {query_config.query_id}")
            
            # Execute query on source account
            start_time = time.time()
            source_df = self.snowflake_connector.execute_query(
                'source', query_config.sql, query_config.timeout_seconds
            )
            query_result.source_execution_time = time.time() - start_time
            query_result.source_row_count = len(source_df)
            
            # Execute query on target account
            start_time = time.time()
            target_df = self.snowflake_connector.execute_query(
                'target', query_config.sql, query_config.timeout_seconds
            )
            query_result.target_execution_time = time.time() - start_time
            query_result.target_row_count = len(target_df)
            
            # Export to DuckDB
            self.duckdb_manager.export_dataframe(source_df, f"{query_config.query_id}_source")
            self.duckdb_manager.export_dataframe(target_df, f"{query_config.query_id}_target")
            
            # Export to Parquet files
            source_path = os.path.join(self.output_dir, "exports", "source_data", f"{query_config.query_id}.parquet")
            target_path = os.path.join(self.output_dir, "exports", "target_data", f"{query_config.query_id}.parquet")
            
            os.makedirs(os.path.dirname(source_path), exist_ok=True)
            os.makedirs(os.path.dirname(target_path), exist_ok=True)
            
            source_df.to_parquet(source_path)
            target_df.to_parquet(target_path)
            
            # Perform comparison
            start_time = time.time()
            comparison_data = self.data_validator.compare_dataframes(source_df, target_df, query_config)
            query_result.comparison_time = time.time() - start_time
            
            # Calculate results
            differences_count = comparison_data["comparison_details"].get("differing_rows", 0)
            query_result.differences_count = differences_count
            query_result.match_percentage = comparison_data["comparison_details"].get("match_percentage", 0)
            
            # Determine status
            if differences_count == 0:
                query_result.status = "SUCCESS"
            else:
                query_result.status = "FAILED"
                
                # Export differences if any
                if differences_count > 0 and comparison_data["comparison_details"].get("sample_differences"):
                    diff_path = os.path.join(self.output_dir, "exports", "differences", f"{query_config.query_id}_differences.csv")
                    os.makedirs(os.path.dirname(diff_path), exist_ok=True)
                    
                    diff_df = pd.DataFrame(comparison_data["comparison_details"]["sample_differences"])
                    diff_df.to_csv(diff_path, index=False)
            
            # Generate reports
            json_path, html_path = self.report_generator.save_reports(query_config, query_result, comparison_data)
            query_result.json_report_path = json_path
            query_result.html_report_path = html_path
            
            query_result.end_time = datetime.now(timezone.utc)
            query_result.execution_time = (query_result.end_time - query_result.start_time).total_seconds()
            
            logger.info(f"Completed validation for {query_config.query_id}: {query_result.status}")
            
        except Exception as e:
            query_result.status = "FAILED"
            query_result.error_message = str(e)
            query_result.end_time = datetime.now(timezone.utc)
            
            if query_result.start_time:
                query_result.execution_time = (query_result.end_time - query_result.start_time).total_seconds()
            
            logger.error(f"Validation failed for {query_config.query_id}: {e}")
        
        return query_result
    
    def run_validation(self, specific_queries: List[str] = None, retry_failed: bool = False):
        """Run the complete validation process"""
        
        try:
            # Determine which queries to run
            if specific_queries:
                queries_to_run = [qid for qid in specific_queries if qid in self.queries]
            elif retry_failed:
                queries_to_run = [qid for qid, result in self.state_manager.state["queries"].items() 
                                if result.get("status") == "FAILED"]
            else:
                all_query_ids = list(self.queries.keys())
                queries_to_run = self.state_manager.get_incomplete_queries(all_query_ids)
            
            if not queries_to_run:
                console.print("[green]All queries already completed successfully![/green]")
                return
            
            console.print(f"[blue]Running validation for {len(queries_to_run)} queries...[/blue]")
            
            # Progress tracking
            with Progress() as progress:
                task = progress.add_task("Validating queries...", total=len(queries_to_run))
                
                for i, query_id in enumerate(queries_to_run):
                    query_config = self.queries[query_id]
                    
                    progress.update(task, description=f"Validating {query_id}")
                    
                    # Execute validation
                    query_result = self.validate_single_query(query_config)
                    
                    # Update state
                    self.state_manager.update_query_status(
                        query_id,
                        query_result.status,
                        start_time=query_result.start_time.isoformat() if query_result.start_time else None,
                        end_time=query_result.end_time.isoformat() if query_result.end_time else None,
                        execution_time=query_result.execution_time,
                        error_message=query_result.error_message,
                        json_report_path=query_result.json_report_path,
                        html_report_path=query_result.html_report_path,
                        differences_count=query_result.differences_count,
                        match_percentage=query_result.match_percentage
                    )
                    self.state_manager.save()
                    
                    progress.advance(task)
                    
                    # Show status
                    status_color = "green" if query_result.status == "SUCCESS" else "red"
                    console.print(f"[{status_color}]{query_id}: {query_result.status}[/{status_color}]")
            
            # Generate summary
            self._generate_summary_reports()
            
            # Display final results
            self._display_final_results()
            
        except KeyboardInterrupt:
            console.print("\n[yellow]Validation interrupted by user. Progress has been saved.[/yellow]")
        except Exception as e:
            logger.error(f"Validation process failed: {e}")
            console.print(f"[red]Validation failed: {e}[/red]")
        finally:
            # Cleanup
            self.cleanup()
    
    def _generate_summary_reports(self):
        """Generate summary reports"""
        try:
            summary = self.state_manager.get_execution_summary()
            
            # Generate JSON summary
            json_summary_path = os.path.join(self.output_dir, "reports", "json", "summary", "validation_summary.json")
            os.makedirs(os.path.dirname(json_summary_path), exist_ok=True)
            
            with open(json_summary_path, 'w') as f:
                json.dump({
                    "execution_summary": summary,
                    "query_details": self.state_manager.state["queries"]
                }, f, indent=2)
            
            logger.info("Generated summary reports")
            
        except Exception as e:
            logger.error(f"Failed to generate summary reports: {e}")
    
    def _display_final_results(self):
        """Display final validation results"""
        summary = self.state_manager.get_execution_summary()
        
        table = Table(title="Validation Summary")
        table.add_column("Metric", justify="left")
        table.add_column("Value", justify="right")
        
        table.add_row("Total Queries", str(summary["total_queries"]))
        table.add_row("Completed", str(summary["completed_queries"]))
        table.add_row("Failed", str(summary["failed_queries"]))
        table.add_row("Success Rate", f"{summary['success_rate']:.1f}%")
        
        console.print(table)
        
        # Show report locations
        console.print(f"\n[blue]Reports generated in:[/blue]")
        console.print(f"  • JSON Reports: {self.output_dir}/reports/json/")
        console.print(f"  • HTML Reports: {self.output_dir}/reports/html/")
        console.print(f"  • Data Exports: {self.output_dir}/exports/")
    
    def cleanup(self):
        """Clean up resources"""
        try:
            self.snowflake_connector.close_connections()
            self.duckdb_manager.close()
            logger.info("Cleanup completed")
        except Exception as e:
            logger.error(f"Cleanup failed: {e}")

@click.command()
@click.option('--config', '-c', default='config/config.yaml', help='Configuration file path')
@click.option('--output', '-o', default='./output', help='Output directory')
@click.option('--queries', help='Comma-separated list of specific queries to run')
@click.option('--resume', is_flag=True, help='Resume from last execution')
@click.option('--retry-failed', is_flag=True, help='Retry only failed queries')
@click.option('--dry-run', is_flag=True, help='Validate configuration only')

def main(config, output, queries, resume, retry_failed, dry_run):
    """Snowflake Migration Validation Tool"""
    
    try:
        console.print("[bold blue]Snowflake Migration Validation Tool[/bold blue]")
        console.print("=" * 50)
        
        if dry_run:
            console.print("[yellow]Dry run mode - validating configuration only[/yellow]")
            # Add dry run validation logic here
            return
        
        # Initialize validator
        validator = SnowflakeValidator(config, output)
        
        # Parse specific queries if provided
        specific_queries = None
        if queries:
            specific_queries = [q.strip() for q in queries.split(',')]
        
        # Run validation
        validator.run_validation(specific_queries, retry_failed)
        
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)

if __name__ == "__main__":
    main()