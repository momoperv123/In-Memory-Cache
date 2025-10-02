#!/usr/bin/env python3
"""
Latency analysis for Redis clone
Detailed latency measurement and analysis tools
"""

import json
import os
import statistics
import sys
import threading
import time
from typing import Any, Dict, List, Tuple

import numpy as np

# Add src to path
sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
)

from redis_clone import Client


class LatencyAnalyzer:
    def __init__(self, host: str = "127.0.0.1", port: int = 31337):
        self.host = host
        self.port = port
        self.latency_samples: List[float] = []

    def create_client(self) -> Client:
        """Create a new client connection"""
        return Client()

    def measure_latency(self, operation_func, *args, **kwargs) -> Tuple[float, Any]:
        """Measure latency of a single operation"""
        start_time = time.perf_counter()
        result = operation_func(*args, **kwargs)
        end_time = time.perf_counter()
        latency = end_time - start_time
        return latency, result

    def measure_latency_batch(
        self, operation_func, num_operations: int, *args, **kwargs
    ) -> List[float]:
        """Measure latency for a batch of operations"""
        latencies = []
        for _ in range(num_operations):
            latency, _ = self.measure_latency(operation_func, *args, **kwargs)
            latencies.append(latency)
        return latencies

    def analyze_set_latency(self, num_operations: int = 10000) -> Dict[str, Any]:
        """Analyze SET operation latency"""
        print(f"Analyzing SET latency ({num_operations} operations)...")

        client = self.create_client()
        client.flush()  # Clear database

        latencies = []
        for i in range(num_operations):
            key = f"latency_test_{i}"
            value = f"value_{i}"
            latency, _ = self.measure_latency(client.set, key, value)
            latencies.append(latency)

        return self._calculate_latency_stats("SET", latencies)

    def analyze_get_latency(self, num_operations: int = 10000) -> Dict[str, Any]:
        """Analyze GET operation latency"""
        print(f"Analyzing GET latency ({num_operations} operations)...")

        client = self.create_client()
        client.flush()  # Clear database

        # Pre-populate with data
        for i in range(num_operations):
            client.set(f"latency_test_{i}", f"value_{i}")

        latencies = []
        for i in range(num_operations):
            key = f"latency_test_{i}"
            latency, _ = self.measure_latency(client.get, key)
            latencies.append(latency)

        return self._calculate_latency_stats("GET", latencies)

    def analyze_ttl_latency(self, num_operations: int = 5000) -> Dict[str, Any]:
        """Analyze TTL operation latency"""
        print(f"Analyzing TTL latency ({num_operations} operations)...")

        client = self.create_client()
        client.flush()  # Clear database

        # Pre-populate with data
        for i in range(num_operations):
            client.set(f"ttl_latency_test_{i}", f"value_{i}")

        latencies = []
        for i in range(num_operations):
            key = f"ttl_latency_test_{i}"
            latency, _ = self.measure_latency(client.execute, "EXPIRE", key, 60)
            latencies.append(latency)

        return self._calculate_latency_stats("TTL", latencies)

    def analyze_mixed_latency(self, num_operations: int = 10000) -> Dict[str, Any]:
        """Analyze mixed operation latency"""
        print(f"Analyzing mixed operation latency ({num_operations} operations)...")

        client = self.create_client()
        client.flush()  # Clear database

        latencies = []
        for i in range(num_operations):
            key = f"mixed_latency_test_{i}"
            value = f"value_{i}"

            # Alternate between SET and GET
            if i % 2 == 0:
                latency, _ = self.measure_latency(client.set, key, value)
            else:
                latency, _ = self.measure_latency(client.get, key)
            latencies.append(latency)

        return self._calculate_latency_stats("MIXED", latencies)

    def analyze_latency_under_load(
        self, num_clients: int = 10, operations_per_client: int = 1000
    ) -> Dict[str, Any]:
        """Analyze latency under concurrent load"""
        print(
            f"Analyzing latency under load ({num_clients} clients, {operations_per_client} ops each)..."
        )

        all_latencies = []
        client_results = []

        def client_worker(client_id: int):
            client = self.create_client()
            client.flush()  # Clear database

            latencies = []
            for i in range(operations_per_client):
                key = f"load_latency_{client_id}_{i}"
                value = f"value_{client_id}_{i}"
                latency, _ = self.measure_latency(client.set, key, value)
                latencies.append(latency)

            client_results.append(
                {
                    "client_id": client_id,
                    "latencies": latencies,
                    "avg_latency_ms": statistics.mean(latencies) * 1000,
                    "min_latency_ms": min(latencies) * 1000,
                    "max_latency_ms": max(latencies) * 1000,
                }
            )

            all_latencies.extend(latencies)

        # Run concurrent clients
        threads = []
        start_time = time.perf_counter()

        for client_id in range(num_clients):
            thread = threading.Thread(target=client_worker, args=(client_id,))
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        end_time = time.perf_counter()
        total_time = end_time - start_time

        # Calculate overall statistics
        overall_stats = self._calculate_latency_stats("LOAD", all_latencies)
        overall_stats["total_time"] = total_time
        overall_stats["client_results"] = client_results

        return overall_stats

    def analyze_latency_distribution(
        self, num_operations: int = 10000
    ) -> Dict[str, Any]:
        """Analyze latency distribution patterns"""
        print(f"Analyzing latency distribution ({num_operations} operations)...")

        client = self.create_client()
        client.flush()  # Clear database

        latencies = []
        for i in range(num_operations):
            key = f"dist_latency_test_{i}"
            value = f"value_{i}"
            latency, _ = self.measure_latency(client.set, key, value)
            latencies.append(latency)

        # Calculate distribution statistics
        latencies_ms = [
            latency * 1000 for latency in latencies
        ]  # Convert to milliseconds

        # Create histogram data
        histogram_bins = 20
        hist, bin_edges = np.histogram(latencies_ms, bins=histogram_bins)

        # Calculate percentiles
        percentiles = {}
        for p in [50, 75, 90, 95, 99, 99.9, 99.99]:
            percentiles[f"p{p}"] = np.percentile(latencies_ms, p)

        return {
            "operation": "DISTRIBUTION",
            "num_operations": num_operations,
            "latencies_ms": latencies_ms,
            "histogram": {
                "bins": bin_edges.tolist(),
                "counts": hist.tolist(),
            },
            "percentiles": percentiles,
            "statistics": self._calculate_latency_stats("DISTRIBUTION", latencies),
        }

    def analyze_latency_trends(
        self, num_operations: int = 10000, sample_interval: int = 100
    ) -> Dict[str, Any]:
        """Analyze latency trends over time"""
        print(
            f"Analyzing latency trends ({num_operations} operations, sampling every {sample_interval})..."
        )

        client = self.create_client()
        client.flush()  # Clear database

        latencies = []
        samples = []

        for i in range(num_operations):
            key = f"trend_latency_test_{i}"
            value = f"value_{i}"
            latency, _ = self.measure_latency(client.set, key, value)
            latencies.append(latency)

            # Sample at intervals
            if i % sample_interval == 0:
                samples.append(
                    {
                        "operation": i,
                        "latency_ms": latency * 1000,
                        "cumulative_avg_ms": statistics.mean(latencies) * 1000,
                    }
                )

        return {
            "operation": "TRENDS",
            "num_operations": num_operations,
            "sample_interval": sample_interval,
            "samples": samples,
            "overall_stats": self._calculate_latency_stats("TRENDS", latencies),
        }

    def analyze_latency_with_different_value_sizes(self) -> Dict[str, Any]:
        """Analyze latency with different value sizes"""
        print("Analyzing latency with different value sizes...")

        client = self.create_client()
        client.flush()  # Clear database

        value_sizes = [1, 10, 100, 1000, 10000]  # KB
        results = {}

        for size_kb in value_sizes:
            print(f"  Testing {size_kb}KB values...")
            value = "x" * (size_kb * 1024)

            latencies = []
            for i in range(1000):  # 1000 operations per size
                key = f"size_test_{size_kb}kb_{i}"
                latency, _ = self.measure_latency(client.set, key, value)
                latencies.append(latency)

            results[f"{size_kb}KB"] = self._calculate_latency_stats(
                f"SET_{size_kb}KB", latencies
            )

        return {
            "operation": "VALUE_SIZES",
            "results": results,
        }

    def _calculate_latency_stats(
        self, operation: str, latencies: List[float]
    ) -> Dict[str, Any]:
        """Calculate comprehensive latency statistics"""
        if not latencies:
            return {"operation": operation, "error": "No latency data"}

        latencies_ms = [
            latency * 1000 for latency in latencies
        ]  # Convert to milliseconds

        # Basic statistics
        stats = {
            "operation": operation,
            "num_samples": len(latencies),
            "avg_latency_ms": statistics.mean(latencies_ms),
            "median_latency_ms": statistics.median(latencies_ms),
            "min_latency_ms": min(latencies_ms),
            "max_latency_ms": max(latencies_ms),
            "std_dev_ms": statistics.stdev(latencies_ms)
            if len(latencies_ms) > 1
            else 0,
        }

        # Percentiles
        percentiles = {}
        for p in [50, 75, 90, 95, 99, 99.9, 99.99]:
            percentiles[f"p{p}"] = np.percentile(latencies_ms, p)

        stats["percentiles"] = percentiles

        # Additional statistics
        stats["variance_ms"] = (
            statistics.variance(latencies_ms) if len(latencies_ms) > 1 else 0
        )
        stats["range_ms"] = max(latencies_ms) - min(latencies_ms)

        # Outlier detection (values beyond 3 standard deviations)
        mean = statistics.mean(latencies_ms)
        std_dev = statistics.stdev(latencies_ms) if len(latencies_ms) > 1 else 0
        outliers = [
            latency for latency in latencies_ms if abs(latency - mean) > 3 * std_dev
        ]
        stats["outliers_count"] = len(outliers)
        stats["outlier_percentage"] = (len(outliers) / len(latencies_ms)) * 100

        return stats

    def run_full_latency_analysis(self) -> Dict[str, Any]:
        """Run complete latency analysis suite"""
        print("=" * 60)
        print("Redis Clone Latency Analysis")
        print("=" * 60)

        analysis_results = {}

        # Run all latency analyses
        analyses = [
            ("SET Latency", lambda: self.analyze_set_latency(10000)),
            ("GET Latency", lambda: self.analyze_get_latency(10000)),
            ("TTL Latency", lambda: self.analyze_ttl_latency(5000)),
            ("Mixed Latency", lambda: self.analyze_mixed_latency(10000)),
            ("Load Latency", lambda: self.analyze_latency_under_load(10, 1000)),
            ("Distribution", lambda: self.analyze_latency_distribution(10000)),
            ("Trends", lambda: self.analyze_latency_trends(10000, 100)),
            ("Value Sizes", lambda: self.analyze_latency_with_different_value_sizes()),
        ]

        for name, analysis_func in analyses:
            try:
                result = analysis_func()
                analysis_results[name] = result
                self._print_latency_analysis(name, result)
            except Exception as e:
                print(f"Error running {name}: {e}")
                analysis_results[name] = {"error": str(e)}

        return analysis_results

    def _print_latency_analysis(self, name: str, result: Dict[str, Any]):
        """Print formatted latency analysis result"""
        if "error" in result:
            print(f"\n{name}: ERROR - {result['error']}")
            return

        print(f"\n{name}:")

        if "statistics" in result:
            stats = result["statistics"]
        else:
            stats = result

        if "num_samples" in stats:
            print(f"  Samples: {stats['num_samples']}")

        if "avg_latency_ms" in stats:
            print(f"  Avg Latency: {stats['avg_latency_ms']:.3f} ms")

        if "median_latency_ms" in stats:
            print(f"  Median Latency: {stats['median_latency_ms']:.3f} ms")

        if "percentiles" in stats:
            percentiles = stats["percentiles"]
            print(f"  P95: {percentiles.get('p95', 0):.3f} ms")
            print(f"  P99: {percentiles.get('p99', 0):.3f} ms")
            print(f"  P99.9: {percentiles.get('p99.9', 0):.3f} ms")

        if "outliers_count" in stats:
            print(
                f"  Outliers: {stats['outliers_count']} ({stats['outlier_percentage']:.2f}%)"
            )


def main():
    """Main latency analyzer runner"""
    analyzer = LatencyAnalyzer()
    results = analyzer.run_full_latency_analysis()

    print("\n" + "=" * 60)
    print("Latency Analysis Complete!")
    print("=" * 60)

    # Save results to file
    import os

    # Create results directory if it doesn't exist
    results_dir = "results"
    os.makedirs(results_dir, exist_ok=True)

    with open(os.path.join(results_dir, "latency_analysis_results.json"), "w") as f:
        json.dump(results, f, indent=2)
    print(f"Results saved to {results_dir}/latency_analysis_results.json")


if __name__ == "__main__":
    main()
