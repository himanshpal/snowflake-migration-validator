# Snowflake Data Migration Validation AI Prompt Specification

## Project Overview
You are tasked with building a comprehensive data migration validation system that compares data and schema between two Snowflake accounts to ensure 100% data integrity post-migration. The system should handle 100+ validation queries using Python, DuckDB for local processing, and reladiff for comparison logic.

## Core Requirements

### 1. Data Export and Materialization
- **Source**: Two Snowflake accounts with identical database/table structures
- **Process**: Export query results to temporary materialized datasets
- **Storage**: Use DuckDB locally for fast comparison operations
- **Scale**: Handle 100+ validation queries efficiently

### 2. Comparison Framework
- **Tool**: Implement reladiff (https://reladiff.readthedocs.io/en/latest/how-to-use.html)
- **Scope**: Both data content and schema structure validation
- **Output**: Detailed diff reports with discrepancy identification

## Technical Implementation Requirements

### Python Architecture
Create a modular system with these components:
1. **SnowflakeConnector**: Handle connections to both Snowflake accounts
2. **QueryManager**: Execute queries sequentially with state management
3. **StateManager**: Track progress, save/load state, handle resume logic
4. **DataExporter**: Export results to DuckDB with proper data type handling
5. **SchemaValidator**: Compare table schemas between accounts
6. **DataValidator**: Use reladiff for row-by-row data comparison
7. **ReportGenerator**: Create per-query and consolidated validation reports
8. **ProgressTracker**: Real-time progress monitoring and ETA calculation

### Configuration Management
Implement a flexible configuration system supporting:
- **Environment-Specific Settings**: Dev, staging, production configurations
- **Snowflake Connections**: Connection pooling, timeout settings, retry policies
- **Query Definitions**: Parameterized queries with variable substitution
- **Validation Rules**: Custom tolerance levels, ignore patterns, business rules
- **Output Preferences**: Report formats, file locations, retention policies
- **Performance Tuning**: Batch sizes, parallel limits, memory allocation
- **Logging Configuration**: Log levels, output destinations, rotation policies

### Query Configuration Format
```yaml
queries:
  query_001:
    name: "customer_count_validation"
    category: "data_volume"
    priority: "high"  # high, medium, low
    timeout_seconds: 300
    retry_attempts: 3
    sql: |
      SELECT COUNT(*) as total_customers 
      FROM ${database}.${schema}.customers 
      WHERE created_date >= '${start_date}'
    comparison_type: "exact_match"
    tolerance: 0  # For numeric comparisons
    required_for_migration: true
    dependencies: []  # Other query IDs that must pass first
    
  query_002:
    name: "order_details_comparison"
    category: "data_content"
    priority: "high"
    timeout_seconds: 1800
    sql: |
      SELECT order_id, customer_id, order_date, total_amount 
      FROM ${database}.${schema}.orders 
      WHERE order_date >= '${validation_start_date}'
      ORDER BY order_id
    comparison_type: "reladiff"
    key_columns: ["order_id"]
    ignore_columns: ["created_timestamp", "modified_timestamp"]  # Skip system columns
    sample_size: 10000  # For large tables, sample for quick validation
    full_validation: true  # Then do full validation if sample passes
```

### Data Processing Pipeline
Design a pipeline that:
1. Connects to both Snowflake accounts
2. Executes queries in parallel where possible
3. Materializes results in DuckDB with proper indexing
4. Performs schema validation first, then data validation
5. Generates detailed reports with statistics and discrepancies

## Specific Implementation Guidelines

### Snowflake Integration
- Use snowflake-connector-python for database connections
- Implement connection pooling for efficiency
- Handle large result sets with proper chunking
- Include proper error handling for network issues

### DuckDB Operations
- Create optimized table structures for fast comparison
- Use appropriate data types mapping from Snowflake
- Implement proper indexing for join operations
- Handle memory management for large datasets

### Performance Optimization Requirements
- **Smart Sampling**: For large tables (>10M rows), implement intelligent sampling strategy
- **Incremental Validation**: Support for validating only changed data since last run
- **Parallel Export**: Multi-threaded data export to DuckDB while maintaining query sequence
- **Memory Management**: Stream large result sets to avoid memory exhaustion
- **Connection Pooling**: Reuse Snowflake connections across queries
- **Caching Strategy**: Cache schema information and small reference tables
- **Progress Estimation**: Provide accurate ETAs based on historical performance

### Monitoring and Observability
```python
class ValidationMetrics:
    def __init__(self):
        self.query_timings = {}
        self.memory_usage = {}
        self.error_counts = {}
        self.data_volumes = {}
    
    def record_query_execution(self, query_id, duration, rows_processed, memory_peak):
        # Store metrics for performance analysis and future ETA calculations
        pass
    
    def generate_performance_report(self):
        # Create detailed performance analysis
        pass
```

### Query Processing Requirements
Design a sequential query processing system that:
- **One Query at a Time**: Process queries sequentially to ensure proper state management
- **Immediate Validation**: Perform comparison immediately after data export
- **Per-Query Reports**: Generate individual validation reports for each query
- **State Persistence**: Save execution status after each query completion
- **Automatic Resume**: Resume processing from the last incomplete query on restart
- **Failure Isolation**: Mark failed queries and continue with remaining queries
- **Revalidation Support**: Allow re-execution of specific queries or all failed queries

### State File Structure
Maintain a JSON state file with the following structure:
```json
{
  "execution_id": "uuid",
  "start_time": "2024-07-25T10:00:00Z",
  "last_updated": "2024-07-25T10:30:00Z",
  "total_queries": 100,
  "completed_queries": 50,
  "failed_queries": 2,
  "current_query_index": 51,
  "queries": {
    "query_001": {
      "name": "customer_count_validation",
      "status": "SUCCESS",
      "execution_time": 45.2,
      "start_time": "2024-07-25T10:00:00Z",
      "end_time": "2024-07-25T10:00:45Z",
      "report_path": "reports/query_001_customer_count_validation.json",
      "error_message": null
    },
    "query_051": {
      "name": "order_details_comparison",
      "status": "FAILED",
      "execution_time": null,
      "start_time": "2024-07-25T10:30:00Z",
      "end_time": null,
      "report_path": null,
      "error_message": "Connection timeout to Snowflake"
    }
  }
}
```

## Expected Deliverables

### Code Structure
```
snowflake_validator/
├── config/
│   ├── connections.yaml
│   ├── queries.yaml
│   └── validation_rules.yaml
├── src/
│   ├── connectors/
│   ├── validators/
│   ├── exporters/
│   ├── reporters/
│   └── utils/
├── queries/
│   ├── schema_validation/
│   ├── data_validation/
│   └── business_logic/
├── reports/
└── tests/
```

### Key Features
1. **Sequential Processing**: Execute queries one at a time with immediate validation
2. **State Persistence**: Save progress after each query completion
3. **Resume Capability**: Automatically resume from last incomplete query
4. **Per-Query Reports**: Individual validation reports for each query
5. **Progress Tracking**: Real-time status updates with completion percentage
6. **Error Recovery**: Continue processing despite individual query failures
7. **Detailed Logging**: Comprehensive logs for debugging and audit trails
8. **Flexible Reporting**: Multiple output formats (JSON, HTML, CSV)
9. **Performance Metrics**: Track execution times and resource usage per query
10. **Revalidation Support**: Re-execute failed or specific queries

### Validation Categories
- **Schema Validation**: Table structures, column definitions, constraints
- **Data Volume**: Row counts, distinct values, aggregations
- **Data Content**: Row-by-row comparison using reladiff
- **Business Logic**: Custom validation rules and data quality checks
- **Performance**: Query execution time comparisons

## Sample Implementation Approach

### 1. Configuration Setup
```python
# Example configuration structure
config = {
    "snowflake": {
        "source": {...},
        "target": {...}
    },
    "duckdb": {
        "database_path": "validation.duckdb",
        "memory_limit": "8GB"
    },
    "validation": {
        "parallel_queries": 10,
        "chunk_size": 100000,
        "tolerance": 0.001
    }
}
```

### 2. Query Definition Format
```yaml
queries:
  - name: "customer_count_validation"
    category: "data_volume"
    sql: "SELECT COUNT(*) as total_customers FROM customers"
    comparison_type: "exact_match"
  
  - name: "order_details_comparison"
    category: "data_content"
    sql: "SELECT * FROM orders WHERE order_date >= '2024-01-01'"
    comparison_type: "reladiff"
    key_columns: ["order_id"]
```

### 3. Validation Workflow
1. Load configuration and query definitions
2. Establish connections to both Snowflake accounts
3. Execute schema validation queries first
4. Run data validation queries in batches
5. Export results to DuckDB with proper staging
6. Use reladiff for detailed data comparison
7. Generate comprehensive validation reports
8. Cleanup temporary resources

## Sample Output Reports

### Individual Query JSON Report (output/reports/json/individual/query_051.json)
```json
{
  "query_info": {
    "query_id": "query_051",
    "query_name": "order_details_comparison",
    "query_sql": "SELECT order_id, customer_id, order_date, total_amount FROM orders WHERE order_date >= '2024-01-01'",
    "category": "data_content",
    "comparison_type": "reladiff"
  },
  "execution_details": {
    "start_time": "2024-07-25T10:30:15Z",
    "end_time": "2024-07-25T10:32:45Z",
    "total_execution_time": 150.3,
    "source_execution_time": 67.2,
    "target_execution_time": 71.1,
    "comparison_time": 12.0
  },
  "data_summary": {
    "source_account": {
      "row_count": 1250000,
      "column_count": 4,
      "data_size_mb": 45.2
    },
    "target_account": {
      "row_count": 1250000,
      "column_count": 4,
      "data_size_mb": 45.2
    }
  },
  "validation_result": {
    "status": "FAILED",
    "overall_match": false,
    "schema_match": true,
    "data_match": false,
    "discrepancy_count": 47,
    "match_percentage": 99.996
  },
  "schema_comparison": {
    "columns_match": true,
    "data_types_match": true,
    "differences": []
  },
  "data_comparison": {
    "reladiff_summary": {
      "total_rows_compared": 1250000,
      "matching_rows": 1249953,
      "differing_rows": 47,
      "source_only_rows": 0,
      "target_only_rows": 0
    },
    "sample_differences": [
      {
        "row_identifier": {"order_id": "ORD-2024-001234"},
        "column": "total_amount",
        "source_value": 299.99,
        "target_value": 300.00,
        "difference_type": "value_mismatch"
      },
      {
        "row_identifier": {"order_id": "ORD-2024-005678"},
        "column": "order_date",
        "source_value": "2024-03-15",
        "target_value": "2024-03-16",
        "difference_type": "value_mismatch"
      }
    ],
    "difference_statistics": {
      "total_amount": {
        "differences_count": 23,
        "avg_difference": 0.01,
        "max_difference": 0.50
      },
      "order_date": {
        "differences_count": 24,
        "difference_pattern": "mostly_1_day_off"
      }
    }
  },
  "recommendations": [
    "Review total_amount calculations in migration logic",
    "Check timezone handling for order_date field",
    "Investigate rounding differences in currency values"
  ],
  "files_generated": {
    "source_export": "output/exports/source_data/query_051.parquet",
    "target_export": "output/exports/target_data/query_051.parquet",
    "differences_export": "output/exports/differences/query_051_differences.csv",
    "json_report": "output/reports/json/individual/query_051.json",
    "html_report": "output/reports/html/individual/query_051.html"
  }
}
```

### Individual Query HTML Report (output/reports/html/individual/query_051.html)
```html
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Query Validation Report: query_051</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 0; padding: 20px; background: #f5f7fa; }
        .container { max-width: 1200px; margin: 0 auto; background: white; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
        .header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; border-radius: 8px 8px 0 0; }
        .status-badge { display: inline-block; padding: 6px 12px; border-radius: 20px; font-weight: bold; margin-left: 10px; }
        .status-failed { background: #ff6b6b; color: white; }
        .status-success { background: #51cf66; color: white; }
        .status-warning { background: #ffd93d; color: #333; }
        .content { padding: 30px; }
        .section { margin-bottom: 30px; padding: 20px; border: 1px solid #e9ecef; border-radius: 6px; }
        .section h3 { margin-top: 0; color: #495057; border-bottom: 2px solid #e9ecef; padding-bottom: 10px; }
        .metrics-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin: 20px 0; }
        .metric-card { background: #f8f9fa; padding: 15px; border-radius: 6px; text-align: center; }
        .metric-value { font-size: 24px; font-weight: bold; color: #495057; }
        .metric-label { font-size: 12px; color: #6c757d; text-transform: uppercase; margin-top: 5px; }
        .table-responsive { overflow-x: auto; }
        table { width: 100%; border-collapse: collapse; margin: 15px 0; }
        th, td { padding: 12px; text-align: left; border-bottom: 1px solid #dee2e6; }
        th { background: #f8f9fa; font-weight: 600; }
        .diff-row { background: #fff5f5; }
        .diff-cell { background: #fed7d7; }
        .recommendations { background: #e6fffa; border-left: 4px solid #38b2ac; padding: 20px; margin: 20px 0; }
        .nav-links { margin: 20px 0; }
        .nav-links a { display: inline-block; padding: 8px 16px; background: #e9ecef; color: #495057; text-decoration: none; border-radius: 4px; margin-right: 10px; }
        .collapsible { cursor: pointer; padding: 10px; background: #f8f9fa; border: none; width: 100%; text-align: left; font-weight: bold; }
        .collapsible:hover { background: #e9ecef; }
        .collapsible-content { display: none; padding: 15px; border-top: 1px solid #dee2e6; }
        .chart-container { height: 300px; margin: 20px 0; }
    </style>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Query Validation Report</h1>
            <h2>query_051: order_details_comparison 
                <span class="status-badge status-failed">FAILED</span>
            </h2>
            <p>Generated: July 25, 2024 at 10:32:45 UTC</p>
        </div>
        
        <div class="content">
            <div class="nav-links">
                <a href="../summary/dashboard.html">← Back to Dashboard</a>
                <a href="query_050.html">← Previous Query</a>
                <a href="query_052.html">Next Query →</a>
                <a href="../../json/individual/query_051.json">View JSON Report</a>
            </div>

            <div class="section">
                <h3>📊 Execution Summary</h3>
                <div class="metrics-grid">
                    <div class="metric-card">
                        <div class="metric-value">2m 30s</div>
                        <div class="metric-label">Total Time</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-value">1.25M</div>
                        <div class="metric-label">Rows Compared</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-value">99.996%</div>
                        <div class="metric-label">Match Rate</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-value">47</div>
                        <div class="metric-label">Differences</div>
                    </div>
                </div>
            </div>

            <div class="section">
                <h3>🗄️ Query Information</h3>
                <table>
                    <tr><th>Query ID</th><td>query_051</td></tr>
                    <tr><th>Query Name</th><td>order_details_comparison</td></tr>
                    <tr><th>Category</th><td>data_content</td></tr>
                    <tr><th>Comparison Type</th><td>reladiff</td></tr>
                    <tr><th>SQL Query</th><td><code>SELECT order_id, customer_id, order_date, total_amount FROM orders WHERE order_date >= '2024-01-01'</code></td></tr>
                </table>
            </div>

            <div class="section">
                <h3>📈 Data Summary</h3>
                <div class="table-responsive">
                    <table>
                        <thead>
                            <tr><th>Account</th><th>Row Count</th><th>Columns</th><th>Data Size (MB)</th></tr>
                        </thead>
                        <tbody>
                            <tr><td>Source</td><td>1,250,000</td><td>4</td><td>45.2</td></tr>
                            <tr><td>Target</td><td>1,250,000</td><td>4</td><td>45.2</td></tr>
                        </tbody>
                    </table>
                </div>
            </div>

            <div class="section">
                <h3>❌ Validation Results</h3>
                <table>
                    <tr><th>Overall Status</th><td><span class="status-badge status-failed">FAILED</span></td></tr>
                    <tr><th>Schema Match</th><td><span class="status-badge status-success">PASSED</span></td></tr>
                    <tr><th>Data Match</th><td><span class="status-badge status-failed">FAILED</span></td></tr>
                    <tr><th>Discrepancy Count</th><td>47 rows (0.004%)</td></tr>
                </table>
            </div>

            <button class="collapsible">🔍 View Sample Differences (47 total)</button>
            <div class="collapsible-content">
                <div class="table-responsive">
                    <table>
                        <thead>
                            <tr><th>Order ID</th><th>Column</th><th>Source Value</th><th>Target Value</th><th>Difference Type</th></tr>
                        </thead>
                        <tbody>
                            <tr class="diff-row">
                                <td>ORD-2024-001234</td>
                                <td class="diff-cell">total_amount</td>
                                <td>299.99</td>
                                <td>300.00</td>
                                <td>value_mismatch</td>
                            </tr>
                            <tr class="diff-row">
                                <td>ORD-2024-005678</td>
                                <td class="diff-cell">order_date</td>
                                <td>2024-03-15</td>
                                <td>2024-03-16</td>
                                <td>value_mismatch</td>
                            </tr>
                            <tr>
                                <td colspan="5" style="text-align: center; padding: 20px;">
                                    <a href="../../exports/differences/query_051_differences.csv">📁 Download Complete Differences (CSV)</a>
                                </td>
                            </tr>
                        </tbody>
                    </table>
                </div>
            </div>

            <div class="section">
                <h3>📊 Difference Analysis</h3>
                <div class="chart-container">
                    <canvas id="differenceChart"></canvas>
                </div>
                <table>
                    <thead>
                        <tr><th>Column</th><th>Differences</th><th>Pattern</th><th>Impact</th></tr>
                    </thead>
                    <tbody>
                        <tr><td>total_amount</td><td>23</td><td>Avg: $0.01, Max: $0.50</td><td>Low - Rounding differences</td></tr>
                        <tr><td>order_date</td><td>24</td><td>Mostly 1 day off</td><td>Medium - Timezone issue</td></tr>
                    </tbody>
                </table>
            </div>

            <div class="recommendations">
                <h3>💡 Recommendations</h3>
                <ul>
                    <li><strong>Total Amount Issues:</strong> Review total_amount calculations in migration logic</li>
                    <li><strong>Date Discrepancies:</strong> Check timezone handling for order_date field</li>
                    <li><strong>Currency Precision:</strong> Investigate rounding differences in currency values</li>
                </ul>
            </div>

            <div class="section">
                <h3>📁 Generated Files</h3>
                <div class="nav-links">
                    <a href="../../exports/source_data/query_051.parquet">Source Data Export</a>
                    <a href="../../exports/target_data/query_051.parquet">Target Data Export</a>
                    <a href="../../exports/differences/query_051_differences.csv">Differences Export</a>
                    <a href="../../json/individual/query_051.json">JSON Report</a>
                </div>
            </div>
        </div>
    </div>

    <script>
        // Collapsible sections
        document.querySelectorAll('.collapsible').forEach(button => {
            button.addEventListener('click', function() {
                this.classList.toggle('active');
                const content = this.nextElementSibling;
                content.style.display = content.style.display === 'block' ? 'none' : 'block';
            });
        });

        // Difference distribution chart
        const ctx = document.getElementById('differenceChart').getContext('2d');
        new Chart(ctx, {
            type: 'doughnut',
            data: {
                labels: ['total_amount', 'order_date'],
                datasets: [{
                    data: [23, 24],
                    backgroundColor: ['#ff6b6b', '#ffd93d'],
                    borderWidth: 0
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    title: {
                        display: true,
                        text: 'Differences by Column'
                    }
                }
            }
        });
    </script>
</body>
</html>
```

### Summary Dashboard HTML (output/reports/html/summary/dashboard.html)
```html
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Snowflake Migration Validation Dashboard</title>
    <style>
        /* Similar CSS with dashboard-specific styling */
        .query-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(300px, 1fr)); gap: 15px; margin: 20px 0; }
        .query-card { border: 1px solid #dee2e6; border-radius: 8px; padding: 15px; background: white; transition: box-shadow 0.3s; }
        .query-card:hover { box-shadow: 0 4px 12px rgba(0,0,0,0.15); }
        .query-card.failed { border-left: 4px solid #ff6b6b; }
        .query-card.success { border-left: 4px solid #51cf66; }
        .search-box { width: 100%; padding: 12px; margin: 20px 0; border: 1px solid #dee2e6; border-radius: 6px; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Migration Validation Dashboard</h1>
            <h2>Execution: validation_2024_07_25_100000</h2>
            <div class="metrics-grid">
                <div class="metric-card">
                    <div class="metric-value">87%</div>
                    <div class="metric-label">Success Rate</div>
                </div>
                <div class="metric-card">
                    <div class="metric-value">87/100</div>
                    <div class="metric-label">Passed</div>
                </div>
                <div class="metric-card">
                    <div class="metric-value">13/100</div>
                    <div class="metric-label">Failed</div>
                </div>
                <div class="metric-card">
                    <div class="metric-value">4h 30m</div>
                    <div class="metric-label">Total Time</div>
                </div>
            </div>
        </div>
        
        <div class="content">
            <input type="text" class="search-box" placeholder="Search queries by ID, name, or status..." id="searchBox">
            
            <div class="query-grid" id="queryGrid">
                <div class="query-card success">
                    <h4><a href="../individual/query_001.html">query_001</a></h4>
                    <p>customer_count_validation</p>
                    <span class="status-badge status-success">PASSED</span>
                    <div class="metric-label">45.2s • 1.2M rows</div>
                </div>
                
                <div class="query-card failed">
                    <h4><a href="../individual/query_051.html">query_051</a></h4>
                    <p>order_details_comparison</p>
                    <span class="status-badge status-failed">FAILED</span>
                    <div class="metric-label">150.3s • 1.25M rows • 47 differences</div>
                </div>
                
                <!-- Additional query cards... -->
            </div>
        </div>
    </div>
    
    <script>
        // Search functionality
        document.getElementById('searchBox').addEventListener('input', function(e) {
            const searchTerm = e.target.value.toLowerCase();
            const cards = document.querySelectorAll('.query-card');
            
            cards.forEach(card => {
                const text = card.textContent.toLowerCase();
                card.style.display = text.includes(searchTerm) ? 'block' : 'none';
            });
        });
    </script>
</body>
</html>
```

### Consolidated Summary Report (validation_summary.json)
```json
{
  "execution_summary": {
    "execution_id": "validation_2024_07_25_100000",
    "start_time": "2024-07-25T10:00:00Z",
    "end_time": "2024-07-25T14:30:00Z",
    "total_execution_time": "4h 30m",
    "total_queries": 100,
    "queries_processed": 100,
    "resumption_count": 2
  },
  "validation_summary": {
    "overall_status": "COMPLETED_WITH_FAILURES",
    "successful_queries": 87,
    "failed_queries": 13,
    "success_rate": 87.0,
    "queries_with_data_differences": 23,
    "queries_with_schema_differences": 2
  },
  "category_breakdown": {
    "schema_validation": {
      "total": 15,
      "passed": 13,
      "failed": 2,
      "success_rate": 86.7
    },
    "data_volume": {
      "total": 25,
      "passed": 25,
      "failed": 0,
      "success_rate": 100.0
    },
    "data_content": {
      "total": 45,
      "passed": 34,
      "failed": 11,
      "success_rate": 75.6
    },
    "business_logic": {
      "total": 15,
      "passed": 15,
      "failed": 0,
      "success_rate": 100.0
    }
  },
  "failed_queries": [
    {
      "query_id": "query_003",
      "query_name": "customer_table_schema",
      "category": "schema_validation",
      "error_type": "schema_mismatch",
      "error_message": "Column 'customer_tier' missing in target account"
    },
    {
      "query_id": "query_051",
      "query_name": "order_details_comparison",
      "category": "data_content",
      "error_type": "data_mismatch",
      "error_message": "47 rows have differing values"
    }
  ],
  "performance_metrics": {
    "avg_query_execution_time": 162.5,
    "fastest_query": {
      "query_id": "query_012",
      "execution_time": 12.3
    },
    "slowest_query": {
      "query_id": "query_089",
      "execution_time": 1247.8
    },
    "total_data_processed_gb": 234.7
  },
  "recommendations": [
    "Review schema differences in customer table",
    "Investigate data quality issues in order processing",
    "Consider implementing data transformation rules for currency rounding",
    "Review timezone settings between environments"
  ],
  "next_steps": [
    "Re-run failed queries after fixing identified issues",
    "Implement data transformation for known differences",
    "Schedule regular validation runs post-migration"
  ]
}
```

### Progress Tracking Output (Console)
```
Snowflake Migration Validation Tool
===================================

Execution ID: validation_2024_07_25_100000
Resume detected: Starting from query 51 of 100

Progress: [████████████████████████████████████████████████████████████████] 100/100 (100%)

Query Status Summary:
✅ Completed: 87 queries
❌ Failed: 13 queries  
⏳ Processing: 0 queries
📊 Success Rate: 87.0%

Current Query: COMPLETED
Total Time: 4h 30m 15s
Estimated Remaining: 0m 0s

Recent Activity:
[14:29:45] ✅ query_100_final_validation - SUCCESS (45.2s)
[14:29:00] ❌ query_099_complex_join - FAILED (12.1s) - Data mismatch: 156 rows
[14:28:15] ✅ query_098_aggregation_check - SUCCESS (67.8s)

Reports Generated:
📁 JSON Reports: output/reports/json/individual/ (100 files)
📁 HTML Reports: output/reports/html/individual/ (100 files)
📁 Summary JSON: output/reports/json/summary/validation_summary.json
📁 Summary HTML: output/reports/html/summary/dashboard.html
📁 Data Exports: output/exports/ (300+ files)

Validation Complete! Check reports/ directory for detailed results.
```

## Error Handling and Resilience Requirements

### Critical Error Scenarios
- **Network Issues**: Snowflake connection drops, timeouts, rate limiting
- **Memory Management**: Large dataset handling, DuckDB memory limits
- **Data Quality**: NULL values, data type mismatches, encoding issues
- **Query Failures**: Syntax errors, permission issues, resource exhaustion
- **File System**: Disk space, permission errors, corrupted exports
- **Concurrent Access**: Multiple process instances, file locking

### Retry and Recovery Strategy
```python
@retry(max_attempts=3, backoff_strategy="exponential")
def execute_query_with_retry(connection, query, query_id):
    try:
        return connection.execute(query)
    except SnowflakeConnectionError as e:
        log.warning(f"Connection error for {query_id}: {e}")
        reconnect_snowflake()
        raise  # Trigger retry
    except QueryTimeoutError as e:
        log.error(f"Query timeout for {query_id}: {e}")
        raise  # Don't retry timeouts
```

### Graceful Degradation
- Continue processing remaining queries when individual queries fail
- Generate partial reports for incomplete validations
- Provide estimated impact of failed validations
- Allow manual override for known acceptable differences

### Reporting Requirements
Generate dual-format reports for maximum usability:

#### **JSON Reports** (Machine-readable)
- **Location**: `output/reports/json/individual/query_{id}.json`
- **Purpose**: Programmatic access, data integration, automated processing
- **Format**: Structured JSON with complete validation details

#### **HTML Reports** (Human-readable)
- **Location**: `output/reports/html/individual/query_{id}.html`
- **Purpose**: Easy visual inspection, stakeholder communication
- **Features**: 
  - Interactive tables with sorting and filtering
  - Color-coded status indicators (green=pass, red=fail, yellow=warning)
  - Expandable sections for detailed differences
  - Charts and visualizations for data distribution
  - Navigation links between related queries
  - Responsive design for mobile/desktop viewing

#### **Report Naming Convention**
- **Individual Reports**: Use exact query_id for easy identification
  - `query_001.json` / `query_001.html`
  - `query_customer_validation.json` / `query_customer_validation.html`
- **Summary Reports**: Descriptive names for overview
  - `validation_summary.json` / `validation_summary.html`
  - `dashboard.html` (interactive overview of all validations)

#### **Report Generation Requirements**
1. Generate both JSON and HTML simultaneously for each query
2. HTML reports should embed CSS/JS for standalone viewing
3. Include navigation breadcrumbs and query metadata
4. Provide direct links to related data export files
5. Auto-refresh capability for real-time monitoring