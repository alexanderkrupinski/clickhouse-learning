#!/usr/bin/env python3
"""
Connect to ClickHouse server and export table data to CSV.
"""

import csv
import sys
from clickhouse_connect import get_client


def export_to_csv(host='localhost', port=8123, database='nyc_taxi', table='trips_small', 
                  output_file='nyc_taxi_data.csv', user='default', password=''):
    """
    Connect to ClickHouse and export table data to CSV.
    
    Args:
        host: ClickHouse server hostname
        port: ClickHouse HTTP port (default 8123)
        database: Database name
        table: Table name
        output_file: Output CSV filename
        user: ClickHouse username
        password: ClickHouse password
    """
    try:
        print(f"Connecting to ClickHouse at {host}:{port}...")
        client = get_client(host=host, port=port, username=user, password=password)
        
        # Check if table exists
        print(f"Checking if table {database}.{table} exists...")
        tables = client.query(f"SHOW TABLES FROM {database}").result_rows
        table_names = [row[0] for row in tables]
        
        if table not in table_names:
            print(f"Error: Table {database}.{table} not found!")
            print(f"Available tables: {table_names}")
            return False
        
        # Get row count
        count_result = client.query(f"SELECT count() FROM {database}.{table}")
        row_count = count_result.result_rows[0][0]
        print(f"Found {row_count:,} rows in {database}.{table}")
        
        if row_count == 0:
            print("Warning: Table is empty. CSV file will only contain headers.")
        
        # Query all data
        print(f"Exporting data to {output_file}...")
        result = client.query(f"SELECT * FROM {database}.{table}")
        
        # Get column names
        column_names = result.column_names
        # Write to CSV file
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            # Write header
            writer.writerow(column_names)
            # Write data rows
            writer.writerows(result.result_rows)
        
        print(f"✓ Successfully exported {row_count:,} rows to {output_file}")
        return True
        
    except ImportError:
        print("Error: clickhouse-connect library not installed.")
        print("Install it with: pip install clickhouse-connect")
        return False
    except Exception as e:
        print(f"Error: {e}")
        return False


def main():
    """Main function."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Export ClickHouse table to CSV')
    parser.add_argument('--host', default='localhost', help='ClickHouse host (default: localhost)')
    parser.add_argument('--port', type=int, default=8123, help='ClickHouse HTTP port (default: 8123)')
    parser.add_argument('--database', default='nyc_taxi', help='Database name (default: nyc_taxi)')
    parser.add_argument('--table', default='trips_small', help='Table name (default: trips_small)')
    parser.add_argument('--output', '-o', default='nyc_taxi_data.csv', help='Output CSV file (default: nyc_taxi_data.csv)')
    parser.add_argument('--user', default='default', help='ClickHouse username (default: default)')
    parser.add_argument('--password', default='', help='ClickHouse password (default: empty)')
    
    args = parser.parse_args()
    
    success = export_to_csv(
        host=args.host,
        port=args.port,
        database=args.database,
        table=args.table,
        output_file=args.output,
        user=args.user,
        password=args.password
    )
    
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
