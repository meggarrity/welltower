#!/usr/bin/env python3
"""
Clear cache/database utility for Welltower API.

Usage:
    python clear_cache.py                    # Clear all data from tables
    python clear_cache.py --delete           # Drop entire database
    python clear_cache.py --db dbname        # Clear specific database
"""

import os
import sys
import psycopg2
import argparse


def get_connection_params(db_name: str):
    """Build PostgreSQL connection parameters"""
    return {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": os.getenv("DB_PORT", "5432"),
        "database": db_name,
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", "postgres"),
    }


def clear_all_data(db_name: str):
    """Clear all data from database tables while keeping schema."""
    try:
        conn = psycopg2.connect(**get_connection_params(db_name))
        cursor = conn.cursor()
        
        tables = ['scd', 'residents', 'units', 'rentroll', 'properties']
        
        for table in tables:
            try:
                cursor.execute(f"DELETE FROM {table}")
                print(f"✓ Cleared {table}")
            except psycopg2.Error as e:
                print(f"⚠ Could not clear {table}: {e}")
        
        conn.commit()
        cursor.close()
        conn.close()
        print(f"✓ Cache cleared from {db_name} database")
        
    except Exception as e:
        print(f"✗ Error clearing cache: {e}")
        sys.exit(1)


def delete_database(db_name: str):
    """Drop entire PostgreSQL database."""
    try:
        # Connect to postgres database to execute DROP DATABASE
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=os.getenv("DB_PORT", "5432"),
            database="postgres",
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASSWORD", "postgres"),
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Terminate all connections to the database
        cursor.execute(f"""
            SELECT pg_terminate_backend(pg_stat_activity.pid)
            FROM pg_stat_activity
            WHERE pg_stat_activity.datname = '{db_name}'
            AND pid <> pg_backend_pid();
        """)
        
        cursor.execute(f"DROP DATABASE IF EXISTS {db_name}")
        print(f"✓ Dropped database: {db_name}")
        
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"✗ Error dropping database: {e}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description='Clear cache/database for Welltower API',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  python clear_cache.py                  # Clear all data from tables
  python clear_cache.py --delete         # Drop entire database
  python clear_cache.py --db welltower   # Clear specific database
  python clear_cache.py --delete --db test_welltower  # Drop specific database
        '''
    )
    
    parser.add_argument(
        '--delete',
        action='store_true',
        help='Drop entire database instead of clearing data'
    )
    
    parser.add_argument(
        '--db',
        default=os.getenv("DB_NAME", "welltower"),
        help=f'Database name (default: {os.getenv("DB_NAME", "welltower")})'
    )
    
    args = parser.parse_args()
    
    if args.delete:
        delete_database(args.db)
    else:
        clear_all_data(args.db)


if __name__ == '__main__':
    main()
