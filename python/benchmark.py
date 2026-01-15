#!/usr/bin/env python3
"""
Compare runtime of pandas import vs direct ClickHouse query
for mean, median, and variance of fare_amount grouped by passenger_count.
"""

import time
import statistics
import pandas as pd
from clickhouse_connect import get_client


def benchmark_pandas_approach(csv_file, output_file, usecols=None, iterations=5, warmup=1):
    """
    Benchmark: Load data from CSV into pandas, compute aggregations, save to CSV.
    
    Args:
        csv_file: Path to input CSV file
        output_file: Path to output CSV file
        usecols: List of columns to load, or None to load all columns
        iterations: Number of benchmark iterations
        warmup: Number of warmup iterations
    """
    case_name = "PANDAS (Selective Columns)" if usecols else "PANDAS (Full CSV)"
    print(f"\n{'='*60}")
    print(case_name)
    print(f"{'='*60}")
    print(f"1. Load data from {csv_file}")
    if usecols:
        print(f"   Loading columns: {usecols}")
    else:
        print("   Loading all columns")
    print("2. Group by passenger_count")
    print("3. Compute mean, median, variance of fare_amount")
    print(f"4. Save results to {output_file}")
    print(f"{'='*60}\n")
    
    load_times = []
    compute_times = []
    total_times = []
    results_df = None
    
    # Warmup
    if warmup > 0:
        print(f"Warming up ({warmup} iterations)...")
        for _ in range(warmup):
            if usecols:
                df = pd.read_csv(csv_file, usecols=usecols)
            else:
                df = pd.read_csv(csv_file)
            _ = df.groupby('passenger_count')['fare_amount'].agg(['mean', 'median', 'var'])
        print("Warmup complete.\n")
    
    # Benchmark
    print(f"Running benchmark ({iterations} iterations)...")
    for i in range(iterations):
        # Time CSV loading
        load_start = time.perf_counter()
        if usecols:
            df = pd.read_csv(csv_file, usecols=usecols)
        else:
            df = pd.read_csv(csv_file)
        load_elapsed = time.perf_counter() - load_start
        
        # Time computation
        compute_start = time.perf_counter()
        grouped = df.groupby('passenger_count')['fare_amount'].agg(['mean', 'median', 'var'])
        grouped.columns = ['mean_fare', 'median_fare', 'variance_fare']
        grouped = grouped.reset_index()
        compute_elapsed = time.perf_counter() - compute_start
        
        total_elapsed = load_elapsed + compute_elapsed
        
        load_times.append(load_elapsed)
        compute_times.append(compute_elapsed)
        total_times.append(total_elapsed)
        
        # Save to CSV (only on last iteration to avoid I/O overhead in timing)
        if i == iterations - 1:
            grouped.to_csv(output_file, index=False)
            results_df = grouped
        
        print(f"  Run {i+1}/{iterations}: Load={load_elapsed*1000:.2f} ms, "
              f"Compute={compute_elapsed*1000:.2f} ms, Total={total_elapsed*1000:.2f} ms")
    
    return {
        'load_times': load_times,
        'compute_times': compute_times,
        'total_times': total_times,
        'load_mean': statistics.mean(load_times),
        'compute_mean': statistics.mean(compute_times),
        'total_mean': statistics.mean(total_times),
        'load_median': statistics.median(load_times),
        'compute_median': statistics.median(compute_times),
        'total_median': statistics.median(total_times),
        'load_min': min(load_times),
        'compute_min': min(compute_times),
        'total_min': min(total_times),
        'load_max': max(load_times),
        'compute_max': max(compute_times),
        'total_max': max(total_times),
        'results': results_df
    }


def benchmark_clickhouse_approach(client, database, table, output_file, iterations=5, warmup=1):
    """
    Benchmark: Compute aggregations directly in ClickHouse, save to CSV.
    """
    print(f"\n{'='*60}")
    print("CLICKHOUSE DIRECT QUERY")
    print(f"{'='*60}")
    print("Compute mean, median, variance directly in ClickHouse")
    print(f"Save results to {output_file}")
    print(f"{'='*60}\n")
    
    query = f"""
        SELECT 
            passenger_count,
            avg(fare_amount) as mean_fare,
            quantile(0.5)(fare_amount) as median_fare,
            varSamp(fare_amount) as variance_fare
        FROM {database}.{table}
        GROUP BY passenger_count
        ORDER BY passenger_count
    """
    
    query_times = []
    save_times = []
    total_times = []
    results_df = None
    
    # Warmup
    if warmup > 0:
        print(f"Warming up ({warmup} iterations)...")
        for _ in range(warmup):
            _ = client.query(query)
        print("Warmup complete.\n")
    
    # Benchmark
    print(f"Running benchmark ({iterations} iterations)...")
    for i in range(iterations):
        # Time query
        query_start = time.perf_counter()
        result = client.query(query)
        query_elapsed = time.perf_counter() - query_start
        
        # Time save (measure on all iterations for consistency)
        save_start = time.perf_counter()
        results_df = pd.DataFrame(
            result.result_rows,
            columns=['passenger_count', 'mean_fare', 'median_fare', 'variance_fare']
        )
        # Only actually write to file on last iteration to avoid I/O overhead
        if i == iterations - 1:
            results_df.to_csv(output_file, index=False)
        save_elapsed = time.perf_counter() - save_start
        
        total_elapsed = query_elapsed + save_elapsed
        
        query_times.append(query_elapsed)
        save_times.append(save_elapsed)
        total_times.append(total_elapsed)
        
        print(f"  Run {i+1}/{iterations}: Query={query_elapsed*1000:.2f} ms, "
              f"Save={save_elapsed*1000:.2f} ms, Total={total_elapsed*1000:.2f} ms")
    
    return {
        'query_times': query_times,
        'save_times': save_times,
        'total_times': total_times,
        'query_mean': statistics.mean(query_times),
        'save_mean': statistics.mean(save_times),
        'total_mean': statistics.mean(total_times),
        'query_median': statistics.median(query_times),
        'save_median': statistics.median(save_times),
        'total_median': statistics.median(total_times),
        'query_min': min(query_times),
        'save_min': min(save_times),
        'total_min': min(total_times),
        'query_max': max(query_times),
        'save_max': max(save_times),
        'total_max': max(total_times),
        'mean': statistics.mean(query_times),  # Keep for backward compatibility
        'results': results_df
    }


def main():
    """Main function."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Compare pandas CSV import vs direct ClickHouse query for fare_amount aggregations'
    )
    parser.add_argument('--host', default='localhost', help='ClickHouse host')
    parser.add_argument('--port', type=int, default=8123, help='ClickHouse HTTP port')
    parser.add_argument('--user', default='default', help='ClickHouse username')
    parser.add_argument('--password', default='', help='ClickHouse password')
    parser.add_argument('--database', default='nyc_taxi', help='Database name')
    parser.add_argument('--table', default='trips_small', help='Table name')
    parser.add_argument('--csv-input', default='data/nyc_taxi_data.csv', help='Input CSV file for pandas')
    parser.add_argument('--csv-output-pandas', default='results_pandas.csv', help='Output CSV for pandas results')
    parser.add_argument('--csv-output-clickhouse', default='results_clickhouse.csv', help='Output CSV for ClickHouse results')
    parser.add_argument('--iterations', '-i', type=int, default=5, help='Number of iterations')
    parser.add_argument('--warmup', '-w', type=int, default=1, help='Number of warmup iterations')
    
    args = parser.parse_args()
    
    # Connect to ClickHouse
    print(f"Connecting to ClickHouse at {args.host}:{args.port}...")
    client = get_client(host=args.host, port=args.port, username=args.user, password=args.password)
    
    # Benchmark pandas approach - Case 1: Selective columns
    pandas_selective_stats = benchmark_pandas_approach(
        args.csv_input, args.csv_output_pandas,
        usecols=['fare_amount', 'passenger_count'],
        iterations=args.iterations, warmup=args.warmup
    )
    
    # Benchmark pandas approach - Case 2: Full CSV
    pandas_full_stats = benchmark_pandas_approach(
        args.csv_input, args.csv_output_pandas.replace('.csv', '_full.csv'),
        usecols=None,
        iterations=args.iterations, warmup=args.warmup
    )
    
    # Benchmark ClickHouse direct query
    clickhouse_stats = benchmark_clickhouse_approach(
        client, args.database, args.table, args.csv_output_clickhouse,
        iterations=args.iterations, warmup=args.warmup
    )
    
    # Print aggregate statistics from all approaches
    print(f"\n{'='*80}")
    print("AGGREGATE STATISTICS")
    print(f"{'='*80}")
    
    print("\nPandas Results (Selective Columns):")
    print(pandas_selective_stats['results'].to_string(index=False))
    
    print("\nPandas Results (Full CSV):")
    print(pandas_full_stats['results'].to_string(index=False))
    
    print("\nClickHouse Results:")
    print(clickhouse_stats['results'].to_string(index=False))
    
    # Print comparison
    print(f"\n{'='*80}")
    print("COMPARISON RESULTS")
    print(f"{'='*80}")
    print(f"{'Approach':<40} {'Load/Query (ms)':<18} {'Compute/Save (ms)':<18} {'Total (ms)':<15}")
    print(f"{'-'*80}")
    print(f"{'Pandas (Selective Columns)':<40} "
          f"{pandas_selective_stats['load_mean']*1000:>15.2f}  "
          f"{pandas_selective_stats['compute_mean']*1000:>15.2f}  "
          f"{pandas_selective_stats['total_mean']*1000:>12.2f}")
    print(f"  (Load: {pandas_selective_stats['load_mean']*1000:.2f} + Compute: {pandas_selective_stats['compute_mean']*1000:.2f})")
    print(f"{'Pandas (Full CSV)':<40} "
          f"{pandas_full_stats['load_mean']*1000:>15.2f}  "
          f"{pandas_full_stats['compute_mean']*1000:>15.2f}  "
          f"{pandas_full_stats['total_mean']*1000:>12.2f}")
    print(f"  (Load: {pandas_full_stats['load_mean']*1000:.2f} + Compute: {pandas_full_stats['compute_mean']*1000:.2f})")
    print(f"{'ClickHouse Direct Query':<40} "
          f"{clickhouse_stats['query_mean']*1000:>15.2f}  "
          f"{clickhouse_stats['save_mean']*1000:>15.2f}  "
          f"{clickhouse_stats['total_mean']*1000:>12.2f}")
    print(f"  (Query: {clickhouse_stats['query_mean']*1000:.2f} + Save: {clickhouse_stats['save_mean']*1000:.2f})")
    print(f"{'='*80}")
    
    # Speedup calculations
    speedup_selective = pandas_selective_stats['total_mean'] / clickhouse_stats['total_mean']
    speedup_full = pandas_full_stats['total_mean'] / clickhouse_stats['total_mean']
    print(f"\nSpeedup vs Pandas (Selective): ClickHouse is {speedup_selective:.2f}x faster")
    print(f"Speedup vs Pandas (Full CSV): ClickHouse is {speedup_full:.2f}x faster")
    print(f"Time saved (Selective): {(pandas_selective_stats['total_mean'] - clickhouse_stats['total_mean'])*1000:.2f} ms per query")
    print(f"Time saved (Full CSV): {(pandas_full_stats['total_mean'] - clickhouse_stats['total_mean'])*1000:.2f} ms per query\n")


if __name__ == '__main__':
    main()
