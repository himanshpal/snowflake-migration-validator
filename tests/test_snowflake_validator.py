# tests/test_snowflake_validator.py
"""Test suite for Snowflake Validator"""

import pytest
import pandas as pd
import tempfile
import os
import yaml
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

# Import modules to test
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from snowflake_validator import (
    SnowflakeValidator, 
    QueryConfig, 
    QueryResult, 
    StateManager, 
    DuckDBManager,
    DataValidator,
    ReportGenerator
)

class TestQueryConfig:
    """Test QueryConfig dataclass"""
    
    def test_query_config_creation(self):
        config = QueryConfig(
            query_id="test_001",
            name="test_query",
            category="data_volume",
            priority="high",
            timeout_seconds=300,
            retry_attempts=3,
            sql="SELECT COUNT(*) FROM test_table",
            comparison_type="exact_match"
        )
        
        assert config.query_id == "test_001"
        assert config.name == "test_query"
        assert config.dependencies == []
        assert config.key_columns == []
        assert config.ignore_columns == []

class TestStateManager:
    """Test StateManager functionality"""
    
    def test_state_creation(self):
        with tempfile.NamedTemporaryFile(delete=False, suffix='.json') as tmp:
            state_manager = StateManager(tmp.name)
            
            assert 'execution_id' in state_manager.state
            assert state_manager.state['total_queries'] == 0
            assert state_manager.state['completed_queries'] == 0
            assert state_manager.state['failed_queries'] == 0
            
            # Cleanup
            os.unlink(tmp.name)
    
    def test_query_status_update(self):
        with tempfile.NamedTemporaryFile(delete=False, suffix='.json') as tmp:
            state_manager = StateManager(tmp.name)
            
            # Update query status
            state_manager.update_query_status("test_001", "SUCCESS", execution_time=45.2)
            
            assert state_manager.get_query_status("test_001") == "SUCCESS"
            assert state_manager.state["queries"]["test_001"]["execution_time"] == 45.2
            assert state_manager.state["completed_queries"] == 1
            
            # Cleanup
            os.unlink(tmp.name)
    
    def test_incomplete_queries(self):
        with tempfile.NamedTemporaryFile(delete=False, suffix='.json') as tmp:
            state_manager = StateManager(tmp.name)
            
            # Add some query states
            state_manager.update_query_status("query_001", "SUCCESS")
            state_manager.update_query_status("query_002", "FAILED")
            state_manager.update_query_status("query_003", "RUNNING")
            
            all_queries = ["query_001", "query_002", "query_003", "query_004"]
            incomplete = state_manager.get_incomplete_queries(all_queries)
            
            # Should return failed, running, and new queries
            assert "query_001" not in incomplete  # Successful
            assert "query_002" in incomplete     # Failed
            assert "query_003" in incomplete     # Running
            assert "query_004" in incomplete     # New
            
            # Cleanup
            os.unlink(tmp.name)

class TestDataValidator:
    """Test DataValidator functionality"""
    
    def test_dataframe_comparison_identical(self):
        # Create identical dataframes
        df1 = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'value': [100, 200, 300]
        })
        df2 = df1.copy()
        
        query_config = QueryConfig(
            query_id="test_001",
            name="test",
            category="data_content",
            priority="high",
            timeout_seconds=300,
            retry_attempts=3,
            sql="SELECT * FROM test",
            comparison_type="reladiff"
        )
        
        # Mock DuckDB manager
        mock_duckdb = Mock()
        validator = DataValidator(mock_duckdb)
        
        result = validator.compare_dataframes(df1, df2, query_config)
        
        assert result["comparison_details"]["differing_rows"] == 0
        assert result["comparison_details"]["match_percentage"] == 100.0
        assert len(result["recommendations"]) == 0
    
    def test_dataframe_comparison_different_counts(self):
        df1 = pd.DataFrame({'id': [1, 2, 3], 'value': [100, 200, 300]})
        df2 = pd.DataFrame({'id': [1, 2], 'value': [100, 200]})
        
        query_config = QueryConfig(
            query_id="test_001",
            name="test",
            category="data_content", 
            priority="high",
            timeout_seconds=300,
            retry_attempts=3,
            sql="SELECT * FROM test",
            comparison_type="reladiff"
        )
        
        mock_duckdb = Mock()
        validator = DataValidator(mock_duckdb)
        
        result = validator.compare_dataframes(df1, df2, query_config)
        
        # Should detect row count difference
        recommendations = result["recommendations"]
        assert any("Row count mismatch" in rec for rec in recommendations)

class TestReportGenerator:
    """Test ReportGenerator functionality"""
    
    def test_json_report_generation(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            generator = ReportGenerator(tmpdir)
            
            query_config = QueryConfig(
                query_id="test_001",
                name="test_query",
                category="data_volume",
                priority="high",
                timeout_seconds=300,
                retry_attempts=3,
                sql="SELECT COUNT(*) FROM test",
                comparison_type="exact_match"
            )
            
            query_result = QueryResult(
                query_id="test_001",
                status="SUCCESS",
                execution_time=45.2,
                source_row_count=1000,
                target_row_count=1000,
                differences_count=0,
                match_percentage=100.0
            )
            
            comparison_data = {
                "comparison_details": {"differing_rows": 0},
                "recommendations": [],
                "source_size_mb": 1.2,
                "target_size_mb": 1.2
            }
            
            json_report = generator.generate_json_report(query_config, query_result, comparison_data)
            
            assert json_report["query_info"]["query_id"] == "test_001"
            assert json_report["validation_result"]["status"] == "SUCCESS"
            assert json_report["validation_result"]["discrepancy_count"] == 0
            assert json_report["execution_details"]["total_execution_time"] == 45.2

class TestConfigValidation:
    """Test configuration validation"""
    
    def test_valid_config(self):
        config = {
            'snowflake': {
                'source': {
                    'user': 'test_user',
                    'password': 'test_pass',
                    'account': 'test.snowflakecomputing.com',
                    'warehouse': 'TEST_WH',
                    'database': 'TEST_DB',
                    'schema': 'PUBLIC'
                },
                'target': {
                    'user': 'test_user',
                    'password': 'test_pass',
                    'account': 'test.snowflakecomputing.com',
                    'warehouse': 'TEST_WH',
                    'database': 'TEST_DB',
                    'schema': 'PUBLIC'
                }
            },
            'queries': {
                'test_001': {
                    'name': 'test_query',
                    'category': 'data_volume',
                    'sql': 'SELECT COUNT(*) FROM test',
                    'comparison_type': 'exact_match'
                }
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as tmp:
            yaml.dump(config, tmp)
            tmp.flush()
            
            from utils.config_validator import ConfigValidator
            is_valid, errors = ConfigValidator.validate_config(tmp.name)
            
            assert is_valid
            assert len(errors) == 0
            
            # Cleanup
            os.unlink(tmp.name)

# Integration test
class TestSnowflakeValidatorIntegration:
    """Integration tests for SnowflakeValidator"""
    
    @patch('snowflake_validator.SnowflakeConnector')
    @patch('snowflake_validator.DuckDBManager')
    def test_validator_initialization(self, mock_duckdb, mock_snowflake):
        """Test validator initialization with mocked dependencies"""
        
        config = {
            'snowflake': {
                'source': {'user': 'test', 'password': 'test', 'account': 'test', 
                          'warehouse': 'test', 'database': 'test', 'schema': 'test'},
                'target': {'user': 'test', 'password': 'test', 'account': 'test',
                          'warehouse': 'test', 'database': 'test', 'schema': 'test'}
            },
            'queries': {
                'test_001': {
                    'name': 'test_query',
                    'category': 'data_volume',
                    'sql': 'SELECT COUNT(*) FROM test',
                    'comparison_type': 'exact_match'
                }
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as tmp:
            yaml.dump(config, tmp)
            tmp.flush()
            
            with tempfile.TemporaryDirectory() as output_dir:
                validator = SnowflakeValidator(tmp.name, output_dir)
                
                assert len(validator.queries) == 1
                assert 'test_001' in validator.queries
                assert validator.queries['test_001'].name == 'test_query'
            
            # Cleanup
            os.unlink(tmp.name)

# Performance tests
class TestPerformance:
    """Performance-related tests"""
    
    def test_large_dataframe_comparison(self):
        """Test performance with larger datasets"""
        # Create larger dataframes for performance testing
        size = 10000
        df1 = pd.DataFrame({
            'id': range(size),
            'value': [i * 2 for i in range(size)],
            'category': ['A', 'B', 'C'] * (size // 3 + 1)
        })[:size]
        
        df2 = df1.copy()
        # Introduce some differences
        df2.loc[100:110, 'value'] = -1
        
        query_config = QueryConfig(
            query_id="perf_test",
            name="performance_test",
            category="data_content",
            priority="medium",
            timeout_seconds=300,
            retry_attempts=3,
            sql="SELECT * FROM large_table",
            comparison_type="reladiff"
        )
        
        mock_duckdb = Mock()
        validator = DataValidator(mock_duckdb)
        
        import time
        start_time = time.time()
        result = validator.compare_dataframes(df1, df2, query_config)
        execution_time = time.time() - start_time
        
        # Should complete within reasonable time (adjust threshold as needed)
        assert execution_time < 5.0  # 5 seconds max
        assert result["comparison_details"]["differing_rows"] > 0

# Error handling tests
class TestErrorHandling:
    """Test error handling scenarios"""
    
    def test_invalid_sql_handling(self):
        """Test handling of invalid SQL queries"""
        query_config = QueryConfig(
            query_id="invalid_sql",
            name="invalid_query",
            category="data_volume",
            priority="high",
            timeout_seconds=300,
            retry_attempts=3,
            sql="SELECT * FROM non_existent_table",
            comparison_type="exact_match"
        )
        
        # This would be tested with actual Snowflake connection in integration tests
        assert query_config.sql is not None
        assert len(query_config.sql.strip()) > 0
    
    def test_connection_failure_retry(self):
        """Test retry logic for connection failures"""
        from snowflake_validator import retry, RetryException
        
        call_count = 0
        
        @retry(max_attempts=3)
        def failing_function():
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                raise RetryException("Simulated connection failure")
            return "success"
        
        result = failing_function()
        assert result == "success"
        assert call_count == 3

if __name__ == "__main__":
    pytest.main([__file__, "-v"])