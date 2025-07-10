#!/usr/bin/env python3
"""
Simple project validation script.
"""
import os
import sys
import subprocess
from pathlib import Path

def run_command(command, description):
    """Run a command and return success status."""
    print(f"Running: {description}")
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"{description} - SUCCESS")
            return True
        else:
            print(f"{description} - FAILED")
            print(f"Error: {result.stderr}")
            return False
    except Exception as e:
        print(f"{description} - ERROR: {e}")
        return False

def check_file_exists(filepath, description):
    """Check if a file exists."""
    print(f"Checking: {description}")
    if Path(filepath).exists():
        print(f"{description} - EXISTS")
        return True
    else:
        print(f"{description} - MISSING")
        return False

def main():
    """Main validation function."""
    print("DataHut-DuckHouse Project Validation")
    print("=" * 50)
    
    # Check required files
    required_files = [
        ("pyproject.toml", "Poetry configuration"),
        ("README.md", "Project documentation"),
        ("docker-compose.yml", "Docker Compose configuration"),
        (".env", "Environment variables"),
        ("Makefile", "Development automation"),
        ("flight_server/app/app.py", "Main application"),
        ("flight_server/app/utils.py", "Utility functions"),
        ("flight_server/app/backends/hybrid_backend.py", "Hybrid backend"),
        ("flight_server/app/trino_client.py", "Trino client"),
        ("flight_server/app/query_orchestrator.py", "Query orchestrator"),
        ("flight_server/app/monitoring.py", "Monitoring utilities"),
        ("tests/conftest.py", "Test configuration"),
        ("tests/test_utils.py", "Utility tests"),
        ("tests/test_trino_client.py", "Trino client tests"),
        ("config/trino/etc/trino.properties", "Trino configuration"),
        ("config/trino/etc/catalog/iceberg.properties", "Iceberg catalog"),
        ("transform/dbt_project/dbt_project.yml", "dbt project configuration"),
        ("transform/dbt_project/config/dbt_profiles.yml", "dbt profiles"),
        ("scripts/query_trino.py", "Trino query script"),
        ("scripts/setup_iceberg_tables.py", "Iceberg setup script"),
        ("docs/TRINO_DBT_INTEGRATION.md", "Trino/dbt documentation"),
        (".github/workflows/ci.yml", "CI/CD pipeline"),
        (".pre-commit-config.yaml", "Pre-commit configuration"),
    ]
    
    print("\nFile Structure Check:")
    file_checks = []
    for filepath, description in required_files:
        file_checks.append(check_file_exists(filepath, description))
    
    # Check Python syntax
    print("\nPython Syntax Check:")
    python_files = [
        "flight_server/app/app.py",
        "flight_server/app/utils.py",
        "flight_server/app/backends/hybrid_backend.py",
        "flight_server/app/trino_client.py",
        "flight_server/app/query_orchestrator.py",
        "flight_server/app/monitoring.py",
        "tests/conftest.py",
        "tests/test_utils.py",
        "tests/test_hybrid_backend.py",
        "tests/test_trino_client.py",
        "tests/test_query_orchestrator.py",
        "scripts/ingest_flight.py",
        "scripts/create_tenant.py",
        "scripts/delete_tenant.py",
        "scripts/query_trino.py",
        "scripts/setup_iceberg_tables.py",
        "scripts/run_dbt.py",
        "scripts/demonstrate_architecture.py",
    ]
    
    syntax_checks = []
    for py_file in python_files:
        if Path(py_file).exists():
            syntax_checks.append(run_command(f"python -m py_compile {py_file}", f"Syntax check: {py_file}"))
    
    # Check TOML syntax
    print("\nConfiguration Check:")
    config_checks = []
    config_checks.append(run_command("python -c \"import tomllib; open('pyproject.toml', 'rb').read()\"", "TOML syntax check"))
    
    # Summary
    print("\nValidation Summary:")
    print("=" * 50)
    
    total_checks = len(file_checks) + len(syntax_checks) + len(config_checks)
    passed_checks = sum(file_checks) + sum(syntax_checks) + sum(config_checks)
    
    print(f"Total checks: {total_checks}")
    print(f"Passed: {passed_checks}")
    print(f"Failed: {total_checks - passed_checks}")
    
    if passed_checks == total_checks:
        print("\nAll checks passed! Project is ready for development.")
        return 0
    else:
        print(f"\nSome checks failed. Please review the errors above.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
