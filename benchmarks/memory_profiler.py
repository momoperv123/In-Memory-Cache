import gc
import json
import os
import sys
import threading
import time
from typing import Any, Dict, List

import psutil

# Add src to path
sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
)

from redis_clone import Client


class MemoryProfiler:
    def __init__(self, host: str = "127.0.0.1", port: int = 31337):
        self.host = host
        self.port = port
        self.memory_samples: List[Dict[str, Any]] = []
        self.process = psutil.Process()

    def get_memory_usage(self) -> Dict[str, float]:
        
        memory_info = self.process.memory_info()
        memory_percent = self.process.memory_percent()

        return {
            "rss_mb": memory_info.rss / 1024 / 1024,  # Resident Set Size
            "vms_mb": memory_info.vms / 1024 / 1024,  # Virtual Memory Size
            "percent": memory_percent,
            "timestamp": time.time(),
        }

    def profile_operation_memory(
        self, operation_name: str, operation_func, *args, **kwargs
    ) -> Dict[str, Any]:
        
        # Force garbage collection before measurement
        gc.collect()

        # Get baseline memory
        baseline = self.get_memory_usage()

        # Perform operation
        start_time = time.perf_counter()
        result = operation_func(*args, **kwargs)
        end_time = time.perf_counter()

        # Get memory after operation
        after_operation = self.get_memory_usage()

        # Force garbage collection and measure again
        gc.collect()
        after_gc = self.get_memory_usage()

        return {
            "operation": operation_name,
            "duration_ms": (end_time - start_time) * 1000,
            "baseline_memory": baseline,
            "after_operation_memory": after_operation,
            "after_gc_memory": after_gc,
            "memory_delta_mb": after_operation["rss_mb"] - baseline["rss_mb"],
            "memory_delta_after_gc_mb": after_gc["rss_mb"] - baseline["rss_mb"],
            "result": result,
        }

    def profile_set_operations(self, num_operations: int = 10000) -> Dict[str, Any]:
        
        print(f"Profiling SET operations memory usage ({num_operations} operations)...")

        client = Client()
        client.flush()  # Clear database

        # Get initial memory
        initial_memory = self.get_memory_usage()

        # Profile individual operations
        operation_profiles = []
        for i in range(min(100, num_operations)):  # Profile first 100 operations
            key = f"mem_test_{i}"
            value = f"value_{i}"
            profile = self.profile_operation_memory("SET", client.set, key, value)
            operation_profiles.append(profile)

        # Perform bulk operations
        start_time = time.perf_counter()
        for i in range(num_operations):
            key = f"bulk_mem_test_{i}"
            value = f"bulk_value_{i}"
            client.set(key, value)
        end_time = time.perf_counter()

        # Get final memory
        final_memory = self.get_memory_usage()

        # Force garbage collection
        gc.collect()
        final_after_gc = self.get_memory_usage()

        return {
            "operation": "SET_BULK",
            "num_operations": num_operations,
            "duration_ms": (end_time - start_time) * 1000,
            "initial_memory": initial_memory,
            "final_memory": final_memory,
            "final_after_gc": final_after_gc,
            "memory_growth_mb": final_memory["rss_mb"] - initial_memory["rss_mb"],
            "memory_growth_after_gc_mb": final_after_gc["rss_mb"]
            - initial_memory["rss_mb"],
            "memory_per_operation_kb": (
                final_memory["rss_mb"] - initial_memory["rss_mb"]
            )
            * 1024
            / num_operations,
            "operation_profiles": operation_profiles,
        }

    def profile_get_operations(self, num_operations: int = 10000) -> Dict[str, Any]:
        
        print(f"Profiling GET operations memory usage ({num_operations} operations)...")

        client = Client()
        client.flush()  # Clear database

        # Pre-populate with data
        for i in range(num_operations):
            client.set(f"get_mem_test_{i}", f"value_{i}")

        # Get initial memory
        initial_memory = self.get_memory_usage()

        # Perform GET operations
        start_time = time.perf_counter()
        for i in range(num_operations):
            key = f"get_mem_test_{i}"
            client.get(key)
        end_time = time.perf_counter()

        # Get final memory
        final_memory = self.get_memory_usage()

        return {
            "operation": "GET_BULK",
            "num_operations": num_operations,
            "duration_ms": (end_time - start_time) * 1000,
            "initial_memory": initial_memory,
            "final_memory": final_memory,
            "memory_growth_mb": final_memory["rss_mb"] - initial_memory["rss_mb"],
        }

    def profile_ttl_operations(self, num_operations: int = 5000) -> Dict[str, Any]:
        
        print(f"Profiling TTL operations memory usage ({num_operations} operations)...")

        client = Client()
        client.flush()  # Clear database

        # Pre-populate with data
        for i in range(num_operations):
            client.set(f"ttl_mem_test_{i}", f"value_{i}")

        # Get initial memory
        initial_memory = self.get_memory_usage()

        # Perform TTL operations
        start_time = time.perf_counter()
        for i in range(num_operations):
            key = f"ttl_mem_test_{i}"
            client.execute("EXPIRE", key, 60)
        end_time = time.perf_counter()

        # Get final memory
        final_memory = self.get_memory_usage()

        return {
            "operation": "TTL_BULK",
            "num_operations": num_operations,
            "duration_ms": (end_time - start_time) * 1000,
            "initial_memory": initial_memory,
            "final_memory": final_memory,
            "memory_growth_mb": final_memory["rss_mb"] - initial_memory["rss_mb"],
        }

    def profile_large_values(
        self, num_operations: int = 1000, value_size_kb: int = 10
    ) -> Dict[str, Any]:
        
        print(
            f"Profiling large values memory usage ({num_operations} operations, {value_size_kb}KB each)..."
        )

        client = Client()
        client.flush()  # Clear database

        # Create large value
        large_value = "x" * (value_size_kb * 1024)

        # Get initial memory
        initial_memory = self.get_memory_usage()

        # Perform operations with large values
        start_time = time.perf_counter()
        for i in range(num_operations):
            key = f"large_mem_test_{i}"
            client.set(key, large_value)
        end_time = time.perf_counter()

        # Get final memory
        final_memory = self.get_memory_usage()

        # Force garbage collection
        gc.collect()
        final_after_gc = self.get_memory_usage()

        return {
            "operation": "LARGE_VALUES",
            "num_operations": num_operations,
            "value_size_kb": value_size_kb,
            "duration_ms": (end_time - start_time) * 1000,
            "initial_memory": initial_memory,
            "final_memory": final_memory,
            "final_after_gc": final_after_gc,
            "memory_growth_mb": final_memory["rss_mb"] - initial_memory["rss_mb"],
            "memory_growth_after_gc_mb": final_after_gc["rss_mb"]
            - initial_memory["rss_mb"],
            "memory_per_operation_kb": (
                final_memory["rss_mb"] - initial_memory["rss_mb"]
            )
            * 1024
            / num_operations,
        }

    def profile_memory_growth_over_time(
        self, duration_seconds: int = 60, operations_per_second: int = 100
    ) -> Dict[str, Any]:
        
        print(
            f"Profiling memory growth over {duration_seconds} seconds at {operations_per_second} ops/sec..."
        )

        client = Client()
        client.flush()  # Clear database

        memory_samples = []
        start_time = time.perf_counter()
        operation_count = 0

        while time.perf_counter() - start_time < duration_seconds:
            # Perform operations
            for _ in range(operations_per_second):
                key = f"growth_test_{operation_count}"
                value = f"value_{operation_count}"
                client.set(key, value)
                operation_count += 1

            # Sample memory
            memory_sample = self.get_memory_usage()
            memory_sample["operation_count"] = operation_count
            memory_sample["elapsed_time"] = time.perf_counter() - start_time
            memory_samples.append(memory_sample)

            # Sleep for 1 second
            time.sleep(1)

        return {
            "operation": "MEMORY_GROWTH",
            "duration_seconds": duration_seconds,
            "operations_per_second": operations_per_second,
            "total_operations": operation_count,
            "memory_samples": memory_samples,
            "initial_memory_mb": memory_samples[0]["rss_mb"] if memory_samples else 0,
            "final_memory_mb": memory_samples[-1]["rss_mb"] if memory_samples else 0,
            "peak_memory_mb": max(sample["rss_mb"] for sample in memory_samples)
            if memory_samples
            else 0,
            "memory_growth_mb": (
                memory_samples[-1]["rss_mb"] - memory_samples[0]["rss_mb"]
            )
            if len(memory_samples) > 1
            else 0,
        }

    def profile_concurrent_memory(
        self, num_clients: int = 10, operations_per_client: int = 1000
    ) -> Dict[str, Any]:
        
        print(
            f"Profiling concurrent memory usage ({num_clients} clients, {operations_per_client} ops each)..."
        )

        # Get initial memory
        initial_memory = self.get_memory_usage()

        def client_worker(client_id: int):
            client = Client()
            client.flush()  # Clear database

            for i in range(operations_per_client):
                key = f"concurrent_mem_{client_id}_{i}"
                value = f"value_{client_id}_{i}"
                client.set(key, value)

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

        # Get final memory
        final_memory = self.get_memory_usage()

        # Force garbage collection
        gc.collect()
        final_after_gc = self.get_memory_usage()

        return {
            "operation": "CONCURRENT_MEMORY",
            "num_clients": num_clients,
            "operations_per_client": operations_per_client,
            "total_operations": num_clients * operations_per_client,
            "duration_ms": (end_time - start_time) * 1000,
            "initial_memory": initial_memory,
            "final_memory": final_memory,
            "final_after_gc": final_after_gc,
            "memory_growth_mb": final_memory["rss_mb"] - initial_memory["rss_mb"],
            "memory_growth_after_gc_mb": final_after_gc["rss_mb"]
            - initial_memory["rss_mb"],
        }

    def run_full_memory_profile(self) -> Dict[str, Any]:
        
        print("=" * 60)
        print("Redis Clone Memory Profiling")
        print("=" * 60)

        profile_results = {}

        # Run all memory profiles
        profiles = [
            ("SET Operations", lambda: self.profile_set_operations(10000)),
            ("GET Operations", lambda: self.profile_get_operations(10000)),
            ("TTL Operations", lambda: self.profile_ttl_operations(5000)),
            ("Large Values", lambda: self.profile_large_values(1000, 10)),
            ("Memory Growth", lambda: self.profile_memory_growth_over_time(30, 100)),
            ("Concurrent Memory", lambda: self.profile_concurrent_memory(10, 1000)),
        ]

        for name, profile_func in profiles:
            try:
                result = profile_func()
                profile_results[name] = result
                self._print_memory_profile(name, result)
            except Exception as e:
                print(f"Error running {name}: {e}")
                profile_results[name] = {"error": str(e)}

        return profile_results

    def _print_memory_profile(self, name: str, result: Dict[str, Any]):
        
        if "error" in result:
            print(f"\n{name}: ERROR - {result['error']}")
            return

        print(f"\n{name}:")
        print(f"  Operations: {result.get('num_operations', 'N/A')}")
        print(f"  Duration: {result.get('duration_ms', 0):.2f} ms")

        if "memory_growth_mb" in result:
            print(f"  Memory Growth: {result['memory_growth_mb']:.2f} MB")

        if "memory_growth_after_gc_mb" in result:
            print(
                f"  Memory Growth (after GC): {result['memory_growth_after_gc_mb']:.2f} MB"
            )

        if "memory_per_operation_kb" in result:
            print(f"  Memory per Operation: {result['memory_per_operation_kb']:.2f} KB")

        if "peak_memory_mb" in result:
            print(f"  Peak Memory: {result['peak_memory_mb']:.2f} MB")


def main():
    
    profiler = MemoryProfiler()
    results = profiler.run_full_memory_profile()

    print("\n" + "=" * 60)
    print("Memory Profiling Complete!")
    print("=" * 60)

    # Save results to file
    import os

    # Create results directory if it doesn't exist
    results_dir = "results"
    os.makedirs(results_dir, exist_ok=True)

    with open(os.path.join(results_dir, "memory_profile_results.json"), "w") as f:
        json.dump(results, f, indent=2)
    print(f"Results saved to {results_dir}/memory_profile_results.json")


if __name__ == "__main__":
    main()
