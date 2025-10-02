#!/usr/bin/env python3
"""
Performance benchmark for Redis clone
Measures operations per second, latency, and throughput
"""

import os
import statistics
import sys
import threading
import time
from typing import Any, Dict, List

# Add src to path
sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
)

from redis_clone import Client


class PerformanceBenchmark:
    def __init__(self, host: str = "127.0.0.1", port: int = 31337):
        self.host = host
        self.port = port
        self.results: Dict[str, List[float]] = {}

    def create_client(self) -> Client:
        """Create a new client connection"""
        return Client()

    def measure_operation(
        self, operation_name: str, operation_func, *args, **kwargs
    ) -> float:
        """Measure the time taken for a single operation"""
        start_time = time.perf_counter()
        _ = operation_func(*args, **kwargs)
        end_time = time.perf_counter()
        return end_time - start_time

    def benchmark_set_operations(self, num_operations: int = 10000) -> Dict[str, Any]:
        """Benchmark SET operations"""
        print(f"Benchmarking SET operations ({num_operations} operations)...")

        client = self.create_client()
        client.flush()  # Clear database

        times = []
        for i in range(num_operations):
            key = f"bench_key_{i}"
            value = f"bench_value_{i}"
            duration = self.measure_operation("SET", client.set, key, value)
            times.append(duration)

        return self._calculate_stats("SET", times, num_operations)

    def benchmark_get_operations(self, num_operations: int = 10000) -> Dict[str, Any]:
        """Benchmark GET operations"""
        print(f"Benchmarking GET operations ({num_operations} operations)...")

        client = self.create_client()
        client.flush()  # Clear database

        # Pre-populate with data
        for i in range(num_operations):
            client.set(f"bench_key_{i}", f"bench_value_{i}")

        times = []
        for i in range(num_operations):
            key = f"bench_key_{i}"
            duration = self.measure_operation("GET", client.get, key)
            times.append(duration)

        return self._calculate_stats("GET", times, num_operations)

    def benchmark_mixed_operations(self, num_operations: int = 10000) -> Dict[str, Any]:
        """Benchmark mixed SET/GET operations"""
        print(f"Benchmarking mixed operations ({num_operations} operations)...")

        client = self.create_client()
        client.flush()  # Clear database

        times = []
        for i in range(num_operations):
            key = f"mixed_key_{i}"
            value = f"mixed_value_{i}"

            # Alternate between SET and GET
            if i % 2 == 0:
                duration = self.measure_operation("SET", client.set, key, value)
            else:
                duration = self.measure_operation("GET", client.get, key)
            times.append(duration)

        return self._calculate_stats("MIXED", times, num_operations)

    def benchmark_ttl_operations(self, num_operations: int = 5000) -> Dict[str, Any]:
        """Benchmark TTL operations"""
        print(f"Benchmarking TTL operations ({num_operations} operations)...")

        client = self.create_client()
        client.flush()  # Clear database

        # Pre-populate with data
        for i in range(num_operations):
            client.set(f"ttl_key_{i}", f"ttl_value_{i}")

        times = []
        for i in range(num_operations):
            key = f"ttl_key_{i}"
            duration = self.measure_operation(
                "EXPIRE", client.execute, "EXPIRE", key, 60
            )
            times.append(duration)

        return self._calculate_stats("TTL", times, num_operations)

    def benchmark_large_values(self, num_operations: int = 1000) -> Dict[str, Any]:
        """Benchmark operations with large values"""
        print(f"Benchmarking large value operations ({num_operations} operations)...")

        client = self.create_client()
        client.flush()  # Clear database

        # Create large value (10KB)
        large_value = "x" * 10240

        times = []
        for i in range(num_operations):
            key = f"large_key_{i}"
            duration = self.measure_operation("SET_LARGE", client.set, key, large_value)
            times.append(duration)

        return self._calculate_stats("LARGE_VALUES", times, num_operations)

    def benchmark_concurrent_clients(
        self, num_clients: int = 10, operations_per_client: int = 1000
    ) -> Dict[str, Any]:
        """Benchmark with multiple concurrent clients"""
        print(
            f"Benchmarking concurrent clients ({num_clients} clients, {operations_per_client} ops each)..."
        )

        results = []
        threads = []

        def client_worker(client_id: int):
            client = self.create_client()
            client.flush()  # Clear database for this client

            times = []
            for i in range(operations_per_client):
                key = f"concurrent_{client_id}_{i}"
                value = f"concurrent_value_{client_id}_{i}"
                duration = self.measure_operation("CONCURRENT", client.set, key, value)
                times.append(duration)

            results.extend(times)

        # Start all client threads
        start_time = time.perf_counter()
        for client_id in range(num_clients):
            thread = threading.Thread(target=client_worker, args=(client_id,))
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        end_time = time.perf_counter()

        total_operations = num_clients * operations_per_client
        total_time = end_time - start_time

        return {
            "operation": "CONCURRENT",
            "total_operations": total_operations,
            "total_time": total_time,
            "ops_per_second": total_operations / total_time,
            "avg_latency_ms": statistics.mean(results) * 1000,
            "min_latency_ms": min(results) * 1000,
            "max_latency_ms": max(results) * 1000,
            "p95_latency_ms": self._percentile(results, 95) * 1000,
            "p99_latency_ms": self._percentile(results, 99) * 1000,
        }

    def _calculate_stats(
        self, operation: str, times: List[float], num_operations: int
    ) -> Dict[str, Any]:
        """Calculate performance statistics"""
        total_time = sum(times)
        ops_per_second = num_operations / total_time

        return {
            "operation": operation,
            "total_operations": num_operations,
            "total_time": total_time,
            "ops_per_second": ops_per_second,
            "avg_latency_ms": statistics.mean(times) * 1000,
            "min_latency_ms": min(times) * 1000,
            "max_latency_ms": max(times) * 1000,
            "median_latency_ms": statistics.median(times) * 1000,
            "p95_latency_ms": self._percentile(times, 95) * 1000,
            "p99_latency_ms": self._percentile(times, 99) * 1000,
            "std_dev_ms": statistics.stdev(times) * 1000 if len(times) > 1 else 0,
        }

    def _percentile(self, data: List[float], percentile: int) -> float:
        """Calculate percentile of data"""
        sorted_data = sorted(data)
        index = int(len(sorted_data) * percentile / 100)
        return sorted_data[min(index, len(sorted_data) - 1)]

    def run_full_benchmark(self) -> Dict[str, Any]:
        """Run complete benchmark suite"""
        print("=" * 60)
        print("Redis Clone Performance Benchmark")
        print("=" * 60)

        benchmark_results = {}

        # Run all benchmarks
        benchmarks = [
            ("SET Operations", lambda: self.benchmark_set_operations(10000)),
            ("GET Operations", lambda: self.benchmark_get_operations(10000)),
            ("Mixed Operations", lambda: self.benchmark_mixed_operations(10000)),
            ("TTL Operations", lambda: self.benchmark_ttl_operations(5000)),
            ("Large Values", lambda: self.benchmark_large_values(1000)),
            ("Concurrent Clients", lambda: self.benchmark_concurrent_clients(10, 1000)),
        ]

        for name, benchmark_func in benchmarks:
            try:
                result = benchmark_func()
                benchmark_results[name] = result
                self._print_benchmark_result(name, result)
            except Exception as e:
                print(f"Error running {name}: {e}")
                benchmark_results[name] = {"error": str(e)}

        return benchmark_results

    def _print_benchmark_result(self, name: str, result: Dict[str, Any]):
        """Print formatted benchmark result"""
        if "error" in result:
            print(f"\n{name}: ERROR - {result['error']}")
            return

        print(f"\n{name}:")
        print(f"  Operations/sec: {result['ops_per_second']:.2f}")
        print(f"  Avg Latency: {result['avg_latency_ms']:.3f} ms")
        print(f"  Min Latency: {result['min_latency_ms']:.3f} ms")
        print(f"  Max Latency: {result['max_latency_ms']:.3f} ms")
        print(f"  P95 Latency: {result['p95_latency_ms']:.3f} ms")
        print(f"  P99 Latency: {result['p99_latency_ms']:.3f} ms")
        if "std_dev_ms" in result:
            print(f"  Std Dev: {result['std_dev_ms']:.3f} ms")


def main():
    """Main benchmark runner"""
    benchmark = PerformanceBenchmark()
    results = benchmark.run_full_benchmark()

    print("\n" + "=" * 60)
    print("Benchmark Complete!")
    print("=" * 60)

    # Save results to file
    import json
    import os

    # Create results directory if it doesn't exist
    results_dir = "results"
    os.makedirs(results_dir, exist_ok=True)

    with open(os.path.join(results_dir, "benchmark_results.json"), "w") as f:
        json.dump(results, f, indent=2)
    print(f"Results saved to {results_dir}/benchmark_results.json")


if __name__ == "__main__":
    main()
