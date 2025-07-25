# utils/config_validator.py
"""Configuration validation utilities"""

import yaml
import sys
from pathlib import Path
from typing import Dict, List, Any, Tuple

class ConfigValidator:
    """Validates configuration files for completeness and correctness"""
    
    REQUIRED_SECTIONS = ['snowflake', 'queries']
    REQUIRED_SNOWFLAKE_FIELDS = ['user', 'password', 'account', 'warehouse', 'database', 'schema']
    REQUIRED_QUERY_FIELDS = ['name', 'category', 'sql', 'comparison_type']
    VALID_CATEGORIES = ['schema_validation', 'data_volume', 'data_content', 'business_logic']
    VALID_COMPARISON_TYPES = ['exact_match', 'reladiff', 'tolerance']
    
    @staticmethod
    def validate_config(config_path: str) -> Tuple[bool, List[str]]:
        """Validate configuration file and return (is_valid, errors)"""
        errors = []
        
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
        except Exception as e:
            return False, [f"Failed to load config file: {e}"]
        
        # Check required sections
        for section in ConfigValidator.REQUIRED_SECTIONS:
            if section not in config:
                errors.append(f"Missing required section: {section}")
        
        # Validate Snowflake configuration
        if 'snowflake' in config:
            for account in ['source', 'target']:
                if account not in config['snowflake']:
                    errors.append(f"Missing Snowflake account configuration: {account}")
                    continue
                
                account_config = config['snowflake'][account]
                for field in ConfigValidator.REQUIRED_SNOWFLAKE_FIELDS:
                    if field not in account_config:
                        errors.append(f"Missing {account} Snowflake field: {field}")
        
        # Validate queries
        if 'queries' in config:
            queries = config['queries']
            if not queries:
                errors.append("No queries defined")
            
            for query_id, query_config in queries.items():
                # Check required fields
                for field in ConfigValidator.REQUIRED_QUERY_FIELDS:
                    if field not in query_config:
                        errors.append(f"Query {query_id} missing required field: {field}")
                
                # Validate category
                if 'category' in query_config:
                    if query_config['category'] not in ConfigValidator.VALID_CATEGORIES:
                        errors.append(f"Query {query_id} has invalid category: {query_config['category']}")
                
                # Validate comparison type
                if 'comparison_type' in query_config:
                    if query_config['comparison_type'] not in ConfigValidator.VALID_COMPARISON_TYPES:
                        errors.append(f"Query {query_id} has invalid comparison_type: {query_config['comparison_type']}")
                
                # Validate SQL
                if 'sql' in query_config:
                    sql = query_config['sql'].strip()
                    if not sql:
                        errors.append(f"Query {query_id} has empty SQL")
                    elif not sql.upper().startswith('SELECT'):
                        errors.append(f"Query {query_id} SQL should start with SELECT")
        
        return len(errors) == 0, errors
