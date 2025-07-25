# README.md
# Snowflake Migration Validation System

A production-ready tool for validating data migration between Snowflake accounts with comprehensive reporting, state management, and resume capabilities.

## Features

- ✅ **Sequential Query Processing** - Process queries one by one with immediate validation
- 📊 **Dual Format Reports** - Generate both JSON and HTML reports for each query
- 🔄 **Resume Capability** - Automatically resume from the last incomplete query on restart
- 📈 **Progress Tracking** - Real-time progress monitoring with rich console output
- 🗃️ **State Management** - Persistent state tracking with automatic checkpointing
- ⚡ **Performance Monitoring** - Track execution times and system resource usage
- 🔍 **Data Comparison** - Uses reladiff for detailed row-by-row comparison
- 🏗️ **Configurable** - YAML-based configuration with environment support
- 🐳 **Docker Support** - Containerized deployment with docker-compose
- 📧 **Notifications** - Email and Slack notifications for validation status

## Quick Start

### 1. Installation

```bash
# Clone the repository
git clone <repository-url>
cd snowflake-migration-validator

# Install dependencies
pip install -r requirements.txt

# Or use the setup script
python scripts/setup_environment.py
```

### 2. Configuration

Copy and modify the sample configuration:

```bash
cp config/config.yaml.example config/config.yaml
# Edit config/config.yaml with your Snowflake credentials and queries
```

### 3. Run Validation

```bash
# Basic validation
python snowflake_validator.py --config config/config.yaml

# Resume from last execution
python snowflake_validator.py --resume

# Retry only failed queries
python snowflake_validator.py --retry-failed

# Run specific queries
python snowflake_validator.py --queries query_001,query_002,query_003
```

## Configuration

### Basic Configuration Structure

```yaml
# Snowflake connection settings
snowflake:
  source:
    user: "SOURCE_USER"
    password: "SOURCE_PASSWORD"
    account: "source_account.snowflakecomputing.com"
    warehouse: "COMPUTE_WH"
    database: "PROD_DB"
    schema: "PUBLIC"
  
  target:
    user: "TARGET_USER"
    password: "TARGET_PASSWORD"
    account: "target_account.snowflakecomputing.com"
    warehouse: "COMPUTE_WH"
    database: "PROD_DB"
    schema: "PUBLIC"

# Query definitions
queries:
  query_001:
    name: "customer_count_validation"
    category: "data_volume"
    priority: "high"
    timeout_seconds: 300
    sql: |
      SELECT COUNT(*) as total_customers 
      FROM customers 
      WHERE created_date >= '2024-01-01'
    comparison_type: "exact_match"
    required_for_migration: true
```

### Query Categories

- **schema_validation** - Compare table schemas and structures
- **data_volume** - Validate row counts and aggregations
- **data_content** - Row-by-row data comparison using reladiff
- **business_logic** - Custom business rule validations

### Comparison Types

- **exact_match** - Values must match exactly
- **reladiff** - Detailed row-by-row comparison with difference reporting
- **tolerance** - Numeric comparison with configurable tolerance

## Output Structure

```
output/
├── reports/
│   ├── json/
│   │   ├── individual/          # query_001.json, query_002.json, etc.
│   │   └── summary/             # validation_summary.json
│   └── html/
│       ├── individual/          # query_001.html, query_002.html, etc.
│       └── summary/             # dashboard.html
├── exports/
│   ├── source_data/             # Source data exports (.parquet)
│   ├── target_data/             # Target data exports (.parquet)
│   └── differences/             # Difference reports (.csv)
└── state/
    └── execution_state.json     # Execution state and progress
```

## Command Line Interface

```bash
# Basic usage
python snowflake_validator.py [OPTIONS]

Options:
  -c, --config PATH       Configuration file path [default: config/config.yaml]
  -o, --output PATH       Output directory [default: ./output]
  --queries TEXT          Comma-separated list of specific queries to run
  --resume                Resume from last execution
  --retry-failed          Retry only failed queries
  --dry-run              Validate configuration only
  --help                 Show this message and exit
```

## Docker Deployment

### Using Docker Compose

```bash
# Build and start services
docker-compose up -d

# Run validation
docker-compose exec snowflake-validator python snowflake_validator.py --config /app/config/production.yaml

# View DuckDB data
docker-compose exec duckdb-viewer duckdb /data/validation.duckdb
```

### Using Docker Directly

```bash
# Build image
docker build -t snowflake-validator .

# Run validation
docker run -v $(pwd)/config:/app/config:ro -v $(pwd)/output:/app/output snowflake-validator \
  --config /app/config/config.yaml
```

## State Management and Resume

The system automatically saves progress after each query completion. If the process is interrupted:

1. **Automatic Detection** - On restart, the system detects existing state files
2. **Resume from Checkpoint** - Automatically resumes from the last incomplete query
3. **Preserve Results** - All completed query results are preserved
4. **Skip Completed** - Successfully completed queries are automatically skipped

### State File Structure

```json
{
  "execution_id": "validation_2024_07_25_100000",
  "start_time": "2024-07-25T10:00:00Z",
  "total_queries": 100,
  "completed_queries": 50,
  "failed_queries": 2,
  "current_query_index": 51,
  "queries": {
    "query_001": {
      "status": "SUCCESS",
      "execution_time": 45.2,
      "differences_count": 0,
      "json_report_path": "output/reports/json/individual/query_001.json",
      "html_report_path": "output/reports/html/individual/query_001.html"
    }
  }
}
```

## Reports

### Individual Query Reports

Each query generates both JSON and HTML reports:

- **JSON Report** - Machine-readable format for automation and integration
- **HTML Report** - Human-readable format with interactive features
- **Naming Convention** - Reports use query_id for easy identification (e.g., `query_001.json`, `query_001.html`)

### HTML Report Features

- 🎨 **Rich Styling** - Professional appearance with responsive design
- 📊 **Interactive Charts** - Visual representations of data differences
- 🔍 **Collapsible Sections** - Expandable detail sections for large datasets
- 🧭 **Navigation** - Links between queries and related files
- 📱 **Mobile-Friendly** - Responsive design for all screen sizes

### Summary Dashboard

- **Execution Overview** - High-level statistics and success rates
- **Query Grid** - Visual grid of all queries with status indicators
- **Search Functionality** - Real-time search across all queries
- **Performance Metrics** - Execution times and resource usage

## Performance Optimization

### Large Dataset Handling

- **Streaming** - Stream large result sets to avoid memory exhaustion
- **Chunking** - Process data in configurable chunks
- **Sampling** - Intelligent sampling for initial validation of large tables
- **Memory Management** - Configurable memory limits for DuckDB

### Connection Management

- **Connection Pooling** - Reuse Snowflake connections across queries
- **Retry Logic** - Exponential backoff for connection failures
- **Timeout Handling** - Configurable timeouts per query
- **Resource Cleanup** - Automatic cleanup of connections and temporary files

## Error Handling

### Retry Strategy

- **Automatic Retries** - Configurable retry attempts with exponential backoff
- **Selective Retrying** - Different retry strategies for different error types
- **Graceful Degradation** - Continue processing remaining queries when individual queries fail

### Failure Recovery

- **Isolation** - Query failures don't affect other queries
- **Detailed Logging** - Comprehensive error logging for debugging
- **Partial Results** - Generate reports for completed queries even if some fail

## Monitoring and Notifications

### Performance Monitoring

- **System Metrics** - Track CPU, memory, and disk usage
- **Query Timing** - Record execution times for each query
- **Resource Utilization** - Monitor peak resource usage
- **Progress Estimation** - Provide accurate ETAs based on historical data

### Notifications

Configure email and Slack notifications:

```yaml
notifications:
  email:
    smtp_server: "smtp.company.com"
    recipients: ["data-team@company.com"]
  slack:
    webhook_url: "https://hooks.slack.com/services/..."
```

## Testing

Run the test suite:

```bash
# Install test dependencies
pip install pytest pytest-cov

# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=snowflake_validator --cov-report=html

# Run specific test categories
pytest tests/ -k "test_state_management" -v
```

## Development

### Project Structure

```
snowflake-migration-validator/
├── snowflake_validator.py          # Main application
├── utils/
│   ├── config_validator.py         # Configuration validation
│   ├── performance_monitor.py      # Performance monitoring
│   ├── notification_service.py     # Email/Slack notifications
│   └── query_generator.py          # Query generation utilities
├── config/                         # Configuration files
├── tests/                         # Test suite
├── scripts/                       # Setup and utility scripts
└── requirements.txt               # Dependencies
```

### Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Add tests for new functionality
5. Run the test suite (`pytest`)
6. Commit your changes (`git commit -m 'Add amazing feature'`)
7. Push to the branch (`git push origin feature/amazing-feature`)
8. Open a Pull Request

## Troubleshooting

### Common Issues

1. **Connection Timeouts**
   ```bash
   # Increase timeout in config
   timeout_seconds: 1800
   ```

2. **Memory Issues**
   ```bash
   # Reduce DuckDB memory limit
   duckdb:
     memory_limit: "4GB"
   ```

3. **Permission Errors**
   ```bash
   # Check Snowflake permissions
   GRANT USAGE ON WAREHOUSE compute_wh TO ROLE analyst_role;
   ```

### Debug Mode

Enable detailed logging:

```bash
export LOG_LEVEL=DEBUG
python snowflake_validator.py --config config/config.yaml
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

For support and questions:

- 📧 Email: data-team@yourcompany.com
- 💬 Slack: #data-migration-support
- 🐛 Issues: [GitHub Issues](https://github.com/yourorg/snowflake-migration-validator/issues)

---

## Changelog

### v1.0.0 (2024-07-25)

- ✨ Initial release
- ✅ Sequential query processing with state management
- 📊 Dual-format reporting (JSON + HTML)
- 🔄 Resume capability and automatic checkpointing
- 🐳 Docker support with docker-compose
- 📧 Email and Slack notifications
- 🧪 Comprehensive test suite
- 📚 Complete documentation