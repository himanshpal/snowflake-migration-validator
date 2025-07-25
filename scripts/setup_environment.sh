# scripts/setup_environment.py
"""Environment setup script"""

import os
import sys
from pathlib import Path
import subprocess
import yaml

def create_directory_structure():
    """Create necessary directory structure"""
    directories = [
        "config",
        "queries",
        "output/exports/source_data",
        "output/exports/target_data", 
        "output/exports/differences",
        "output/reports/json/individual",
        "output/reports/json/summary",
        "output/reports/html/individual",
        "output/reports/html/summary",
        "output/state",
        "output/logs",
        "tmp",
        "tests",
        "utils"
    ]
    
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        print(f"Created directory: {directory}")

def check_python_version():
    """Check Python version compatibility"""
    if sys.version_info < (3, 8):
        print("ERROR: Python 3.8 or higher is required")
        sys.exit(1)
    print(f"✓ Python version {sys.version_info.major}.{sys.version_info.minor} is compatible")

def install_dependencies():
    """Install required dependencies"""
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
        print("✓ Dependencies installed successfully")
    except subprocess.CalledProcessError as e:
        print(f"ERROR: Failed to install dependencies: {e}")
        sys.exit(1)

def create_sample_config():
    """Create sample configuration file"""
    config_path = Path("config/config.yaml")
    if config_path.exists():
        print("✓ Configuration file already exists")
        return
    
    sample_config = {
        'snowflake': {
            'source': {
                'user': 'YOUR_SOURCE_USER',
                'password': 'YOUR_SOURCE_PASSWORD',
                'account': 'your_source_account.snowflakecomputing.com',
                'warehouse': 'COMPUTE_WH',
                'database': 'PROD_DB',
                'schema': 'PUBLIC'
            },
            'target': {
                'user': 'YOUR_TARGET_USER',
                'password': 'YOUR_TARGET_PASSWORD',
                'account': 'your_target_account.snowflakecomputing.com',
                'warehouse': 'COMPUTE_WH',
                'database': 'PROD_DB',
                'schema': 'PUBLIC'
            }
        },
        'queries': {
            'query_001': {
                'name': 'sample_validation',
                'category': 'data_volume',
                'sql': 'SELECT COUNT(*) as total_rows FROM your_table',
                'comparison_type': 'exact_match'
            }
        }
    }
    
    with open(config_path, 'w') as f:
        yaml.dump(sample_config, f, default_flow_style=False, indent=2)
    
    print(f"✓ Created sample configuration: {config_path}")
    print("  Please update with your actual Snowflake credentials and queries")

def main():
    """Main setup function"""
    print("Setting up Snowflake Migration Validator environment...")
    print("=" * 50)
    
    check_python_version()
    create_directory_structure()
    install_dependencies()
    create_sample_config()
    
    print("\n" + "=" * 50)
    print("✓ Environment setup completed successfully!")
    print("\nNext steps:")
    print("1. Update config/config.yaml with your Snowflake credentials")
    print("2. Add your validation queries to the config file")
    print("3. Run: python snowflake_validator.py --config config/config.yaml")

if __name__ == "__main__":
    main()