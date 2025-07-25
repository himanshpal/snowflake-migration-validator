# utils/query_generator.py
"""Utilities for generating validation queries"""

import yaml
from typing import Dict, List, Any
from pathlib import Path

class QueryGenerator:
    """Generates validation queries from table metadata"""
    
    @staticmethod
    def generate_schema_validation_query(table_name: str, database: str = None, 
                                       schema: str = None) -> Dict[str, Any]:
        """Generate schema validation query for a table"""
        
        where_clause = f"WHERE table_name = '{table_name.upper()}'"
        if database:
            where_clause += f" AND table_catalog = '{database.upper()}'"
        if schema:
            where_clause += f" AND table_schema = '{schema.upper()}'"
        
        return {
            'name': f"{table_name.lower()}_schema_validation",
            'category': 'schema_validation',
            'priority': 'high',
            'timeout_seconds': 60,
            'sql': f"""
                SELECT 
                    column_name,
                    data_type,
                    is_nullable,
                    column_default,
                    ordinal_position
                FROM information_schema.columns 
                {where_clause}
                ORDER BY ordinal_position
            """.strip(),
            'comparison_type': 'exact_match',
            'required_for_migration': True
        }
    
    @staticmethod
    def generate_row_count_query(table_name: str, where_clause: str = None) -> Dict[str, Any]:
        """Generate row count validation query"""
        
        sql = f"SELECT COUNT(*) as row_count FROM {table_name}"
        if where_clause:
            sql += f" WHERE {where_clause}"
        
        return {
            'name': f"{table_name.lower()}_row_count",
            'category': 'data_volume',
            'priority': 'high',
            'timeout_seconds': 300,
            'sql': sql,
            'comparison_type': 'exact_match',
            'required_for_migration': True
        }
    
    @staticmethod
    def generate_data_sample_query(table_name: str, sample_size: int = 10000,
                                 key_columns: List[str] = None) -> Dict[str, Any]:
        """Generate data sampling query for comparison"""
        
        order_by = ""
        if key_columns:
            order_by = f"ORDER BY {', '.join(key_columns)}"
        
        return {
            'name': f"{table_name.lower()}_data_sample",
            'category': 'data_content',
            'priority': 'medium',
            'timeout_seconds': 900,
            'sql': f"""
                SELECT * FROM {table_name}
                {order_by}
                LIMIT {sample_size}
            """.strip(),
            'comparison_type': 'reladiff',
            'key_columns': key_columns or [],
            'sample_size': sample_size,
            'required_for_migration': False
        }
    
    @staticmethod
    def generate_queries_from_tables(tables: List[str], output_file: str = None) -> Dict[str, Any]:
        """Generate a complete set of validation queries for given tables"""
        
        queries = {}
        query_counter = 1
        
        for table in tables:
            # Schema validation query
            query_id = f"query_{query_counter:03d}"
            queries[query_id] = QueryGenerator.generate_schema_validation_query(table)
            query_counter += 1
            
            # Row count query
            query_id = f"query_{query_counter:03d}"
            queries[query_id] = QueryGenerator.generate_row_count_query(table)
            query_counter += 1
            
            # Data sample query
            query_id = f"query_{query_counter:03d}"
            queries[query_id] = QueryGenerator.generate_data_sample_query(table)
            query_counter += 1
        
        if output_file:
            # Save to YAML file
            config = {'queries': queries}
            with open(output_file, 'w') as f:
                yaml.dump(config, f, default_flow_style=False, indent=2)
        
        return queries
