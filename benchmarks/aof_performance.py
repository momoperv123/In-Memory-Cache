import json
import os
import threading
import time
from typing import Any, Dict

from src.redis_clone.aof import AOFManager, FsyncPolicy


class AOFPerformanceBenchmark:
    """AOF performance benchmarking suite"""

    def __init__(self):
        self.results = {}

    def benchmark_fsync_policies(self, num_operations: int = 10000) -> Dict[str, Any]:
        """Benchmark different fsync policies"""
        print(f"Benchmarking fsync policies with {num_operations} operations...")

        policies = [FsyncPolicy.ALWAYS, FsyncPolicy.EVERYSEC, FsyncPolicy.NO]
        policy_results = {}

        for policy in policies:
            aof_file = f"test_aof_{policy.value}.aof"

            # Clean up
            if os.path.exists(aof_file):
                os.remove(aof_file)

            # Create AOF manager
            aof_manager = AOFManager(aof_file, policy)
            aof_manager.start()

            # Benchmark write performance
            start_time = time.perf_counter()
            for i in range(num_operations):
                aof_manager.append_command("SET", f"key{i}", f"value{i}")
            end_time = time.perf_counter()

            write_time = end_time - start_time
            ops_per_second = num_operations / write_time

            # Get file size
            file_size = aof_manager.get_file_size()

            aof_manager.stop()

            # Benchmark replay performance
            replay_start = time.perf_counter()
            aof_manager = AOFManager(aof_file, policy)
            commands_replayed = aof_manager.replay_commands(lambda *args: None)
            replay_end = time.perf_counter()

            replay_time = replay_end - replay_start
            replay_ops_per_second = (
                commands_replayed / replay_time if replay_time > 0 else 0
            )

            policy_results[policy.value] = {
                "write_time": write_time,
                "write_ops_per_second": ops_per_second,
                "replay_time": replay_time,
                "replay_ops_per_second": replay_ops_per_second,
                "file_size_bytes": file_size,
                "commands_replayed": commands_replayed,
            }

            # Clean up
            if os.path.exists(aof_file):
                os.remove(aof_file)

        return {
            "test": "fsync_policies_benchmark",
            "num_operations": num_operations,
            "results": policy_results,
        }

    def benchmark_large_dataset_replay(
        self, num_operations: int = 200000
    ) -> Dict[str, Any]:
        """Benchmark replay performance with large dataset"""
        print(f"Benchmarking large dataset replay with {num_operations} operations...")

        aof_file = "test_large_dataset.aof"

        # Clean up
        if os.path.exists(aof_file):
            os.remove(aof_file)

        # Create and populate AOF file
        aof_manager = AOFManager(aof_file, FsyncPolicy.EVERYSEC)
        aof_manager.start()

        print("Writing operations to AOF...")
        write_start = time.perf_counter()
        for i in range(num_operations):
            if i % 10000 == 0:
                print(f"Written {i}/{num_operations} operations...")
            aof_manager.append_command("SET", f"key{i}", f"value{i}")
        write_end = time.perf_counter()

        write_time = write_end - write_start
        file_size = aof_manager.get_file_size()

        aof_manager.stop()

        # Benchmark replay
        print("Replaying operations from AOF...")
        replay_start = time.perf_counter()
        aof_manager = AOFManager(aof_file, FsyncPolicy.EVERYSEC)
        commands_replayed = aof_manager.replay_commands(lambda *args: None)
        replay_end = time.perf_counter()

        replay_time = replay_end - replay_start
        replay_ops_per_second = (
            commands_replayed / replay_time if replay_time > 0 else 0
        )

        # Clean up
        if os.path.exists(aof_file):
            os.remove(aof_file)

        return {
            "test": "large_dataset_replay",
            "num_operations": num_operations,
            "write_time": write_time,
            "replay_time": replay_time,
            "replay_ops_per_second": replay_ops_per_second,
            "file_size_bytes": file_size,
            "commands_replayed": commands_replayed,
            "replay_under_2s": replay_time < 2.0,
        }

    def benchmark_concurrent_writes(
        self, num_threads: int = 10, operations_per_thread: int = 1000
    ) -> Dict[str, Any]:
        """Benchmark concurrent AOF writes"""
        print(
            f"Benchmarking concurrent writes: {num_threads} threads, {operations_per_thread} ops each..."
        )

        aof_file = "test_concurrent.aof"

        # Clean up
        if os.path.exists(aof_file):
            os.remove(aof_file)

        # Create AOF manager
        aof_manager = AOFManager(aof_file, FsyncPolicy.EVERYSEC)
        aof_manager.start()

        results = []
        threads = []

        def worker(thread_id: int):
            start_time = time.perf_counter()
            for i in range(operations_per_thread):
                aof_manager.append_command(
                    "SET", f"key_{thread_id}_{i}", f"value_{thread_id}_{i}"
                )
            end_time = time.perf_counter()

            results.append(
                {
                    "thread_id": thread_id,
                    "time_taken": end_time - start_time,
                    "ops_per_second": operations_per_thread / (end_time - start_time),
                }
            )

        # Start threads
        start_time = time.perf_counter()
        for i in range(num_threads):
            thread = threading.Thread(target=worker, args=(i,))
            threads.append(thread)
            thread.start()

        # Wait for all threads
        for thread in threads:
            thread.join()

        end_time = time.perf_counter()

        total_time = end_time - start_time
        total_operations = num_threads * operations_per_thread
        overall_ops_per_second = total_operations / total_time

        aof_manager.stop()

        # Clean up
        if os.path.exists(aof_file):
            os.remove(aof_file)

        return {
            "test": "concurrent_writes",
            "num_threads": num_threads,
            "operations_per_thread": operations_per_thread,
            "total_operations": total_operations,
            "total_time": total_time,
            "overall_ops_per_second": overall_ops_per_second,
            "thread_results": results,
        }

    def benchmark_mixed_operations(self, num_operations: int = 10000) -> Dict[str, Any]:
        """Benchmark mixed operation types"""
        print(f"Benchmarking mixed operations with {num_operations} operations...")

        aof_file = "test_mixed.aof"

        # Clean up
        if os.path.exists(aof_file):
            os.remove(aof_file)

        # Create AOF manager
        aof_manager = AOFManager(aof_file, FsyncPolicy.EVERYSEC)
        aof_manager.start()

        operation_counts = {"SET": 0, "DELETE": 0, "EXPIRE": 0, "FLUSH": 0}

        start_time = time.perf_counter()
        for i in range(num_operations):
            op_type = i % 4

            if op_type == 0:  # SET
                aof_manager.append_command("SET", f"key{i}", f"value{i}")
                operation_counts["SET"] += 1
            elif op_type == 1:  # DELETE
                aof_manager.append_command("DELETE", f"key{i - 1}")
                operation_counts["DELETE"] += 1
            elif op_type == 2:  # EXPIRE
                aof_manager.append_command("EXPIRE", f"key{i}", "3600")
                operation_counts["EXPIRE"] += 1
            elif op_type == 3:  # FLUSH (every 1000 operations)
                if i % 1000 == 0:
                    aof_manager.append_command("FLUSH")
                    operation_counts["FLUSH"] += 1

        end_time = time.perf_counter()

        write_time = end_time - start_time
        file_size = aof_manager.get_file_size()

        aof_manager.stop()

        # Benchmark replay
        replay_start = time.perf_counter()
        aof_manager = AOFManager(aof_file, FsyncPolicy.EVERYSEC)
        commands_replayed = aof_manager.replay_commands(lambda *args: None)
        replay_end = time.perf_counter()

        replay_time = replay_end - replay_start

        # Clean up
        if os.path.exists(aof_file):
            os.remove(aof_file)

        return {
            "test": "mixed_operations",
            "num_operations": num_operations,
            "write_time": write_time,
            "replay_time": replay_time,
            "file_size_bytes": file_size,
            "commands_replayed": commands_replayed,
            "operation_counts": operation_counts,
        }

    def run_full_benchmark(self) -> Dict[str, Any]:
        """Run all AOF performance benchmarks"""
        print("Starting AOF Performance Benchmark Suite")
        print("=" * 60)

        benchmarks = [
            self.benchmark_fsync_policies,
            self.benchmark_large_dataset_replay,
            self.benchmark_concurrent_writes,
            self.benchmark_mixed_operations,
        ]

        results = {}

        for benchmark_func in benchmarks:
            try:
                result = benchmark_func()
                results[result["test"]] = result
                print(f"{result['test']}: Completed")
            except Exception as e:
                results[benchmark_func.__name__] = {
                    "test": benchmark_func.__name__,
                    "error": str(e),
                }
                print(f"{benchmark_func.__name__}: Failed - {e}")

        print("\n" + "=" * 60)
        print("AOF Performance Benchmark Complete")
        print("=" * 60)

        return {
            "benchmark_suite": "aof_performance",
            "timestamp": time.time(),
            "results": results,
        }


def main():
    """Run AOF performance benchmarks"""
    benchmark = AOFPerformanceBenchmark()

    try:
        results = benchmark.run_full_benchmark()

        # Save results
        import os

        os.makedirs("results", exist_ok=True)

        with open("results/aof_performance_results.json", "w") as f:
            json.dump(results, f, indent=2)

        print("\nResults saved to results/aof_performance_results.json")

        return 0

    except Exception as e:
        print(f"Benchmark failed: {e}")
        return 1


if __name__ == "__main__":
    exit(main())
