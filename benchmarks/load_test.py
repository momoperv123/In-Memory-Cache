import json
import os
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List

# Add src to path
sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
)

from redis_clone import Client


class LoadTester:
    def __init__(self, host: str = "127.0.0.1", port: int = 31337):
        self.host = host
        self.port = port
        self.results: List[Dict[str, Any]] = []

    def create_client(self) -> Client:
        
        return Client()

    def worker_set_operations(
        self, worker_id: int, num_operations: int
    ) -> Dict[str, Any]:
        
        client = self.create_client()
        client.flush()  # Clear database

        start_time = time.perf_counter()
        success_count = 0
        error_count = 0
        latencies = []

        for i in range(num_operations):
            try:
                key = f"load_test_{worker_id}_{i}"
                value = f"value_{worker_id}_{i}"

                op_start = time.perf_counter()
                result = client.set(key, value)
                op_end = time.perf_counter()

                if result == 1:
                    success_count += 1
                    latencies.append(op_end - op_start)
                else:
                    error_count += 1

            except Exception as e:
                error_count += 1
                print(f"Worker {worker_id} error: {e}")

        end_time = time.perf_counter()
        total_time = end_time - start_time

        return {
            "worker_id": worker_id,
            "operation": "SET",
            "total_operations": num_operations,
            "success_count": success_count,
            "error_count": error_count,
            "total_time": total_time,
            "ops_per_second": num_operations / total_time,
            "avg_latency_ms": statistics.mean(latencies) * 1000 if latencies else 0,
            "min_latency_ms": min(latencies) * 1000 if latencies else 0,
            "max_latency_ms": max(latencies) * 1000 if latencies else 0,
        }

    def worker_get_operations(
        self, worker_id: int, num_operations: int
    ) -> Dict[str, Any]:
        
        client = self.create_client()
        client.flush()  # Clear database

        # Pre-populate with data
        for i in range(num_operations):
            client.set(f"load_test_{worker_id}_{i}", f"value_{worker_id}_{i}")

        start_time = time.perf_counter()
        success_count = 0
        error_count = 0
        latencies = []

        for i in range(num_operations):
            try:
                key = f"load_test_{worker_id}_{i}"

                op_start = time.perf_counter()
                result = client.get(key)
                op_end = time.perf_counter()

                if result is not None:
                    success_count += 1
                    latencies.append(op_end - op_start)
                else:
                    error_count += 1

            except Exception as e:
                error_count += 1
                print(f"Worker {worker_id} error: {e}")

        end_time = time.perf_counter()
        total_time = end_time - start_time

        return {
            "worker_id": worker_id,
            "operation": "GET",
            "total_operations": num_operations,
            "success_count": success_count,
            "error_count": error_count,
            "total_time": total_time,
            "ops_per_second": num_operations / total_time,
            "avg_latency_ms": statistics.mean(latencies) * 1000 if latencies else 0,
            "min_latency_ms": min(latencies) * 1000 if latencies else 0,
            "max_latency_ms": max(latencies) * 1000 if latencies else 0,
        }

    def worker_mixed_operations(
        self, worker_id: int, num_operations: int
    ) -> Dict[str, Any]:
        
        client = self.create_client()
        client.flush()  # Clear database

        start_time = time.perf_counter()
        success_count = 0
        error_count = 0
        latencies = []

        for i in range(num_operations):
            try:
                key = f"mixed_test_{worker_id}_{i}"
                value = f"value_{worker_id}_{i}"

                op_start = time.perf_counter()

                # Alternate between SET and GET
                if i % 2 == 0:
                    result = client.set(key, value)
                    expected_success = result == 1
                else:
                    result = client.get(key)
                    expected_success = result is not None

                op_end = time.perf_counter()

                if expected_success:
                    success_count += 1
                    latencies.append(op_end - op_start)
                else:
                    error_count += 1

            except Exception as e:
                error_count += 1
                print(f"Worker {worker_id} error: {e}")

        end_time = time.perf_counter()
        total_time = end_time - start_time

        return {
            "worker_id": worker_id,
            "operation": "MIXED",
            "total_operations": num_operations,
            "success_count": success_count,
            "error_count": error_count,
            "total_time": total_time,
            "ops_per_second": num_operations / total_time,
            "avg_latency_ms": statistics.mean(latencies) * 1000 if latencies else 0,
            "min_latency_ms": min(latencies) * 1000 if latencies else 0,
            "max_latency_ms": max(latencies) * 1000 if latencies else 0,
        }

    def run_load_test(
        self, num_workers: int, operations_per_worker: int, operation_type: str = "SET"
    ) -> Dict[str, Any]:
        
        print(
            f"Starting load test: {num_workers} workers, {operations_per_worker} ops each, {operation_type} operations"
        )

        # Choose worker function based on operation type
        worker_functions = {
            "SET": self.worker_set_operations,
            "GET": self.worker_get_operations,
            "MIXED": self.worker_mixed_operations,
        }

        if operation_type not in worker_functions:
            raise ValueError(f"Unknown operation type: {operation_type}")

        worker_func = worker_functions[operation_type]

        # Run load test
        start_time = time.perf_counter()
        results = []

        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            # Submit all worker tasks
            futures = [
                executor.submit(worker_func, worker_id, operations_per_worker)
                for worker_id in range(num_workers)
            ]

            # Collect results as they complete
            for future in as_completed(futures):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    print(f"Worker failed: {e}")

        end_time = time.perf_counter()
        total_time = end_time - start_time

        # Aggregate results
        total_operations = sum(r["total_operations"] for r in results)
        total_success = sum(r["success_count"] for r in results)
        total_errors = sum(r["error_count"] for r in results)

        # Calculate aggregate latency stats from worker results
        all_avg_latencies = []
        all_min_latencies = []
        all_max_latencies = []
        for result in results:
            if "avg_latency_ms" in result:
                all_avg_latencies.append(
                    result["avg_latency_ms"] / 1000
                )  # Convert back to seconds
            if "min_latency_ms" in result:
                all_min_latencies.append(result["min_latency_ms"] / 1000)
            if "max_latency_ms" in result:
                all_max_latencies.append(result["max_latency_ms"] / 1000)

        return {
            "test_config": {
                "num_workers": num_workers,
                "operations_per_worker": operations_per_worker,
                "operation_type": operation_type,
                "total_operations": total_operations,
            },
            "performance": {
                "total_time": total_time,
                "overall_ops_per_second": total_operations / total_time,
                "success_rate": (total_success / total_operations) * 100
                if total_operations > 0
                else 0,
                "error_rate": (total_errors / total_operations) * 100
                if total_operations > 0
                else 0,
            },
            "latency_stats": {
                "avg_latency_ms": statistics.mean(all_avg_latencies) * 1000
                if all_avg_latencies
                else 0,
                "min_latency_ms": min(all_min_latencies) * 1000
                if all_min_latencies
                else 0,
                "max_latency_ms": max(all_max_latencies) * 1000
                if all_max_latencies
                else 0,
                "median_latency_ms": statistics.median(all_avg_latencies) * 1000
                if all_avg_latencies
                else 0,
                "p95_latency_ms": self._percentile(all_avg_latencies, 95) * 1000
                if all_avg_latencies
                else 0,
                "p99_latency_ms": self._percentile(all_avg_latencies, 99) * 1000
                if all_avg_latencies
                else 0,
                "std_dev_ms": statistics.stdev(all_avg_latencies) * 1000
                if len(all_avg_latencies) > 1
                else 0,
            },
            "worker_results": results,
        }

    def _percentile(self, data: List[float], percentile: int) -> float:
        
        if not data:
            return 0
        sorted_data = sorted(data)
        index = int(len(sorted_data) * percentile / 100)
        return sorted_data[min(index, len(sorted_data) - 1)]

    def run_scaling_test(self) -> Dict[str, Any]:
        
        print("Running scaling test...")

        scaling_results = {}
        worker_counts = [1, 2, 5, 10, 20, 50]
        operations_per_worker = 1000

        for num_workers in worker_counts:
            print(f"Testing with {num_workers} workers...")
            result = self.run_load_test(num_workers, operations_per_worker, "SET")
            scaling_results[f"{num_workers}_workers"] = result

            # Print quick summary
            perf = result["performance"]
            latency = result["latency_stats"]
            print(
                f"  Ops/sec: {perf['overall_ops_per_second']:.2f}, "
                f"Avg Latency: {latency['avg_latency_ms']:.3f}ms, "
                f"Success Rate: {perf['success_rate']:.1f}%"
            )

        return scaling_results

    def run_sustained_load_test(
        self, duration_seconds: int = 60, num_workers: int = 10
    ) -> Dict[str, Any]:
        
        print(
            f"Running sustained load test for {duration_seconds} seconds with {num_workers} workers..."
        )

        results = []
        start_time = time.perf_counter()

        def sustained_worker(worker_id: int):
            client = self.create_client()
            operation_count = 0
            error_count = 0
            latencies = []

            while time.perf_counter() - start_time < duration_seconds:
                try:
                    key = f"sustained_{worker_id}_{operation_count}"
                    value = f"value_{worker_id}_{operation_count}"

                    op_start = time.perf_counter()
                    result = client.set(key, value)
                    op_end = time.perf_counter()

                    if result == 1:
                        latencies.append(op_end - op_start)
                    else:
                        error_count += 1

                    operation_count += 1

                except Exception as e:
                    error_count += 1
                    print(f"Sustained worker {worker_id} error: {e}")

            return {
                "worker_id": worker_id,
                "operations": operation_count,
                "errors": error_count,
            }

        # Run sustained test
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = [
                executor.submit(sustained_worker, worker_id)
                for worker_id in range(num_workers)
            ]

            for future in as_completed(futures):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    print(f"Sustained worker failed: {e}")

        end_time = time.perf_counter()
        actual_duration = end_time - start_time

        # Aggregate results
        total_operations = sum(r["operations"] for r in results)
        total_errors = sum(r["errors"] for r in results)

        return {
            "test_config": {
                "duration_seconds": duration_seconds,
                "actual_duration": actual_duration,
                "num_workers": num_workers,
            },
            "performance": {
                "total_operations": total_operations,
                "ops_per_second": total_operations / actual_duration,
                "success_rate": ((total_operations - total_errors) / total_operations)
                * 100
                if total_operations > 0
                else 0,
                "error_rate": (total_errors / total_operations) * 100
                if total_operations > 0
                else 0,
            },
            "latency_stats": {
                "avg_latency_ms": 0,  # Not available for sustained test
                "min_latency_ms": 0,
                "max_latency_ms": 0,
                "p95_latency_ms": 0,
                "p99_latency_ms": 0,
            },
            "worker_results": results,
        }


def main():
    
    print("=" * 60)
    print("Redis Clone Load Testing")
    print("=" * 60)

    tester = LoadTester()

    # Run different load tests
    test_results = {}

    # 1. Basic load test
    print("\n1. Basic Load Test (10 workers, 1000 ops each)")
    basic_result = tester.run_load_test(10, 1000, "SET")
    test_results["basic_load"] = basic_result

    # 2. Scaling test
    print("\n2. Scaling Test")
    scaling_result = tester.run_scaling_test()
    test_results["scaling"] = scaling_result

    # 3. Sustained load test
    print("\n3. Sustained Load Test (30 seconds)")
    sustained_result = tester.run_sustained_load_test(30, 10)
    test_results["sustained"] = sustained_result

    # 4. Mixed operations test
    print("\n4. Mixed Operations Load Test")
    mixed_result = tester.run_load_test(10, 1000, "MIXED")
    test_results["mixed_operations"] = mixed_result

    # Save results
    import os

    # Create results directory if it doesn't exist
    results_dir = "results"
    os.makedirs(results_dir, exist_ok=True)

    with open(os.path.join(results_dir, "load_test_results.json"), "w") as f:
        json.dump(test_results, f, indent=2)

    print("\n" + "=" * 60)
    print("Load Testing Complete!")
    print(f"Results saved to {results_dir}/load_test_results.json")
    print("=" * 60)


if __name__ == "__main__":
    main()
