#!/usr/bin/env python3
"""
Run dbt to transform and test the analytics stack.
"""
import subprocess
import sys


def run_dbt_command(command, description):
    """Run a dbt command and capture the output."""
    print(f"Running: {description}")
    try:
        result = subprocess.run(command, shell=True)
        if result.returncode == 0:
            print(f"✅ {description} - SUCCESS")
        else:
            print(f"❌ {description} - FAILED")
            sys.exit(1)
    except Exception as e:
        print(f"❌ {description} - ERROR: {e}")
        sys.exit(1)


def main():
    """Main function to run dbt transformations."""
    print("🔄 Running dbt transformations...")

    # Run dbt seed
    run_dbt_command("dbt seed --profiles-dir config --project-dir .", "DBT Seed")

    # Run dbt run
    run_dbt_command("dbt run --profiles-dir config --project-dir .", "DBT Run")

    # Run dbt test
    run_dbt_command("dbt test --profiles-dir config --project-dir .", "DBT Test")

    print("🎉 All dbt tasks completed successfully!")


if __name__ == "__main__":
    main()
