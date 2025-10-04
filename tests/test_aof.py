import json
import os
import subprocess
import time
from typing import Any, Dict

from src.redis_clone.aof import AOFManager, FsyncPolicy
from src.redis_clone.client import Client


class AOFTestSuite:

    def __init__(self):
        self.test_results = {}
        self.aof_file = "test_redis_clone.aof"
        self.server_process = None
        self.server_port = 31338  # Different port for testing

    def cleanup(self):
        if self.server_process:
            try:
                self.server_process.terminate()
                self.server_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.server_process.kill()
            except Exception:
                pass
            self.server_process = None

        if os.path.exists(self.aof_file):
            os.remove(self.aof_file)

    def test_basic_aof_functionality(self) -> Dict[str, Any]:
        print("Testing basic AOF functionality...")

        self.cleanup()

        aof_manager = AOFManager(self.aof_file, FsyncPolicy.ALWAYS)
        aof_manager.start()

        commands_logged = []
        aof_manager.append_command("SET", "key1", "value1")
        commands_logged.append(("SET", "key1", "value1"))

        aof_manager.append_command("SET", "key2", "value2")
        commands_logged.append(("SET", "key2", "value2"))

        aof_manager.append_command("DELETE", "key1")
        commands_logged.append(("DELETE", "key1"))

        aof_manager.stop()

        replayed_commands = []

        def mock_handler(command: str, *args: str):
            replayed_commands.append((command,) + args)

        aof_manager = AOFManager(self.aof_file, FsyncPolicy.ALWAYS)
        commands_replayed = aof_manager.replay_commands(mock_handler)

        success = (
            commands_replayed == len(commands_logged)
            and replayed_commands == commands_logged
        )

        self.cleanup()

        return {
            "test": "basic_aof_functionality",
            "success": success,
            "commands_logged": len(commands_logged),
            "commands_replayed": commands_replayed,
            "expected_commands": commands_logged,
            "actual_commands": replayed_commands,
        }

    def test_fsync_policies(self) -> Dict[str, Any]:
        """Test different fsync policies"""
        print("Testing fsync policies...")

        results = {}

        for policy in [FsyncPolicy.ALWAYS, FsyncPolicy.EVERYSEC, FsyncPolicy.NO]:
            self.cleanup()

            aof_manager = AOFManager(self.aof_file, policy)
            aof_manager.start()

            start_time = time.time()
            for i in range(100):
                aof_manager.append_command("SET", f"key{i}", f"value{i}")
            end_time = time.time()

            aof_manager.stop()

            file_exists = os.path.exists(self.aof_file)
            file_size = os.path.getsize(self.aof_file) if file_exists else 0

            results[policy.value] = {
                "time_taken": end_time - start_time,
                "file_exists": file_exists,
                "file_size": file_size,
            }

        self.cleanup()

        return {
            "test": "fsync_policies",
            "success": all(
                r["file_exists"] and r["file_size"] > 0 for r in results.values()
            ),
            "results": results,
        }

    def test_corruption_recovery(self) -> Dict[str, Any]:
        print("Testing corruption recovery...")

        self.cleanup()

        aof_manager = AOFManager(self.aof_file, FsyncPolicy.ALWAYS)
        aof_manager.start()

        aof_manager.append_command("SET", "key1", "value1")
        aof_manager.append_command("SET", "key2", "value2")
        aof_manager.append_command("SET", "key3", "value3")

        aof_manager.stop()

        with open(self.aof_file, "a", encoding="utf-8") as f:
            f.write("INVALID_DATA_HERE\r\n")
            f.write("*2\r\n$3\r\nSET\r\n$4\r\nkey4\r\n")  # Incomplete command

        replayed_commands = []

        def mock_handler(command: str, *args: str):
            replayed_commands.append((command,) + args)

        aof_manager = AOFManager(self.aof_file, FsyncPolicy.ALWAYS)
        commands_replayed = aof_manager.replay_commands(mock_handler)

        expected_commands = [
            ("SET", "key1", "value1"),
            ("SET", "key2", "value2"),
            ("SET", "key3", "value3"),
        ]
        success = commands_replayed == 3 and replayed_commands == expected_commands

        self.cleanup()

        return {
            "test": "corruption_recovery",
            "success": success,
            "commands_replayed": commands_replayed,
            "expected_commands": expected_commands,
            "actual_commands": replayed_commands,
        }

    def test_power_off_simulation(self) -> Dict[str, Any]:
        print("Testing power-off simulation...")

        self.cleanup()

        server_cmd = [
            "python",
            "-c",
            f"from src.redis_clone.server import Server; Server(port={self.server_port}, aof_file='{self.aof_file}').run()",
        ]

        self.server_process = subprocess.Popen(
            server_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )

        # Wait for server to start
        time.sleep(2)

        client = Client()
        client.connect("127.0.0.1", self.server_port)

        operations_performed = []
        try:
            client.set("key1", "value1")
            operations_performed.append(("SET", "key1", "value1"))

            client.set("key2", "value2")
            operations_performed.append(("SET", "key2", "value2"))

            client.delete("key1")
            operations_performed.append(("DELETE", "key1"))

            self.server_process.terminate()
            time.sleep(1)

        except Exception as e:
            print(f"Client operations failed: {e}")

        aof_exists = os.path.exists(self.aof_file)
        aof_size = os.path.getsize(self.aof_file) if aof_exists else 0

        # Restart server and check data recovery
        self.server_process = subprocess.Popen(
            server_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )

        time.sleep(2)

        recovered_data = {}
        try:
            client = Client()
            client.connect("127.0.0.1", self.server_port)

            # Check if key2 still exists (key1 should be deleted)
            value2 = client.get("key2")
            if value2:
                recovered_data["key2"] = value2

            # Check if key1 is gone
            value1 = client.get("key1")
            recovered_data["key1"] = value1  # Should be None

        except Exception as e:
            print(f"Recovery check failed: {e}")

        self.cleanup()

        success = (
            aof_exists
            and aof_size > 0
            and recovered_data.get("key2") == "value2"
            and recovered_data.get("key1") is None
        )

        return {
            "test": "power_off_simulation",
            "success": success,
            "aof_exists": aof_exists,
            "aof_size": aof_size,
            "operations_performed": len(operations_performed),
            "recovered_data": recovered_data,
        }

    def test_performance_benchmark(self) -> Dict[str, Any]:
        print("Testing AOF performance benchmark...")

        self.cleanup()

        # Test replay performance
        aof_manager = AOFManager(self.aof_file, FsyncPolicy.EVERYSEC)
        aof_manager.start()

        start_time = time.time()
        for i in range(200000):
            aof_manager.append_command("SET", f"key{i}", f"value{i}")
        end_time = time.time()

        aof_manager.stop()

        replayed_commands = []

        def mock_handler(command: str, *args: str):
            replayed_commands.append((command,) + args)

        replay_start = time.time()
        aof_manager = AOFManager(self.aof_file, FsyncPolicy.EVERYSEC)
        commands_replayed = aof_manager.replay_commands(mock_handler)
        replay_end = time.time()

        replay_time = replay_end - replay_start

        self.cleanup()

        success = replay_time < 2.0 and commands_replayed == 200000

        return {
            "test": "performance_benchmark",
            "success": success,
            "write_time": end_time - start_time,
            "replay_time": replay_time,
            "commands_replayed": commands_replayed,
            "replay_under_2s": replay_time < 2.0,
        }

    def run_all_tests(self) -> Dict[str, Any]:
        print("Starting AOF Test Suite")
        print("=" * 60)

        tests = [
            self.test_basic_aof_functionality,
            self.test_fsync_policies,
            self.test_corruption_recovery,
            self.test_power_off_simulation,
            self.test_performance_benchmark,
        ]

        results = {}
        passed = 0
        failed = 0

        for test_func in tests:
            try:
                result = test_func()
                results[result["test"]] = result
                if result["success"]:
                    passed += 1
                    print(f"{result['test']}: PASSED")
                else:
                    failed += 1
                    print(f"{result['test']}: FAILED")
            except Exception as e:
                failed += 1
                results[test_func.__name__] = {
                    "test": test_func.__name__,
                    "success": False,
                    "error": str(e),
                }
                print(f"{test_func.__name__}: ERROR - {e}")

        self.cleanup()

        summary = {
            "total_tests": len(tests),
            "passed": passed,
            "failed": failed,
            "results": results,
        }

        print("\n" + "=" * 60)
        print("AOF Test Suite Complete")
        print(f"Total: {len(tests)}, Passed: {passed}, Failed: {failed}")
        print("=" * 60)

        return summary


def main():
    test_suite = AOFTestSuite()

    try:
        results = test_suite.run_all_tests()

        with open("aof_test_results.json", "w") as f:
            json.dump(results, f, indent=2)

        print("\nResults saved to aof_test_results.json")

        if results["failed"] > 0:
            print(f"\n{results['failed']} test(s) failed!")
            return 1
        else:
            print("\n All AOF tests passed!")
            return 0

    except Exception as e:
        print(f"Test suite failed: {e}")
        return 1
    finally:
        test_suite.cleanup()


if __name__ == "__main__":
    exit(main())
