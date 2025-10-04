import json
import os
import subprocess
import sys
import time
from typing import Any, Dict

# Add src to path
sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
)


class PerformanceTestRunner:
    def __init__(self):
        self.test_scripts = [
            "performance_benchmark.py",
            "load_test.py",
            "memory_profiler.py",
            "latency_analyzer.py",
            "aof_performance.py",
        ]
        self.analysis_scripts = [
            "optimization_analyzer.py",
            "performance_comparison.py",
        ]
        self.results = {}

    def check_server_running(self) -> bool:
        
        try:
            import socket

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)
            result = sock.connect_ex(("127.0.0.1", 31337))
            sock.close()
            return result == 0
        except OSError:
            return False

    def run_script(self, script_name: str) -> Dict[str, Any]:
        
        print(f"\n{'=' * 60}")
        print(f"Running {script_name}")
        print(f"{'=' * 60}")

        script_path = os.path.join(os.path.dirname(__file__), script_name)

        try:
            # Run the script
            result = subprocess.run(
                [sys.executable, script_path],
                capture_output=True,
                text=True,
                cwd=os.path.dirname(__file__),
            )

            if result.returncode == 0:
                print(f" {script_name} completed successfully")
                return {
                    "status": "success",
                    "stdout": result.stdout,
                    "stderr": result.stderr,
                }
            else:
                print(f" {script_name} failed with return code {result.returncode}")
                print(f"Error output: {result.stderr}")
                return {
                    "status": "error",
                    "returncode": result.returncode,
                    "stdout": result.stdout,
                    "stderr": result.stderr,
                }

        except Exception as e:
            print(f" {script_name} failed with exception: {e}")
            return {
                "status": "exception",
                "error": str(e),
            }

    def run_all_tests(self) -> Dict[str, Any]:
        
        print(" Starting Redis Clone Performance Testing Suite")
        print("=" * 80)

        # Clean and create results directory
        import os
        import shutil

        results_dir = "results"
        if os.path.exists(results_dir):
            shutil.rmtree(results_dir)
        os.makedirs(results_dir)
        print(f"Cleaned and created {results_dir}/ directory")

        # Check if server is running
        if not self.check_server_running():
            print(
                "  Warning: Redis clone server doesn't appear to be running on 127.0.0.1:31337"
            )
            print("   Please start the server with: python main.py")
            print("   Continuing with tests anyway...")

        start_time = time.time()

        # Run all test scripts
        for script in self.test_scripts:
            self.results[script] = self.run_script(script)
            time.sleep(1)  # Brief pause between tests

        # Run analysis scripts
        for script in self.analysis_scripts:
            self.results[script] = self.run_script(script)
            time.sleep(1)  # Brief pause between tests

        end_time = time.time()
        total_time = end_time - start_time

        # Create summary
        summary = {
            "total_time": total_time,
            "tests_run": len(self.test_scripts),
            "analyses_run": len(self.analysis_scripts),
            "successful_tests": 0,
            "failed_tests": 0,
            "results": self.results,
        }

        # Count successes and failures
        for _, result in self.results.items():
            if result["status"] == "success":
                summary["successful_tests"] += 1
            else:
                summary["failed_tests"] += 1

        return summary

    def print_summary(self, summary: Dict[str, Any]):
        
        print("\n" + "=" * 80)
        print("PERFORMANCE TESTING SUITE SUMMARY")
        print("=" * 80)

        print(f"Total Execution Time: {summary['total_time']:.2f} seconds")
        print(f"Tests Run: {summary['tests_run']}")
        print(f"Analyses Run: {summary['analyses_run']}")
        print(f"Successful: {summary['successful_tests']}")
        print(f"Failed: {summary['failed_tests']}")

        print("\n DETAILED RESULTS:")
        print("-" * 40)

        for script, result in summary["results"].items():
            status_icon = "" if result["status"] == "success" else ""
            print(f"{status_icon} {script}: {result['status']}")

            if result["status"] != "success":
                if "error" in result:
                    print(f"   Error: {result['error']}")
                elif "returncode" in result:
                    print(f"   Return code: {result['returncode']}")
                    if result.get("stderr"):
                        print(f"   Error output: {result['stderr'][:200]}...")

        print("\n GENERATED FILES:")
        print("-" * 40)

        # List generated result files
        results_dir = "results"
        result_files = [
            "benchmark_results.json",
            "load_test_results.json",
            "memory_profile_results.json",
            "latency_analysis_results.json",
            "aof_performance_results.json",
            "performance_analysis_report.json",
            "performance_comparison_report.json",
            "test_execution_summary.json",
            "baseline_performance.json",
        ]

        if os.path.exists(results_dir):
            print(f"Results directory: {results_dir}/")
            for filename in result_files:
                filepath = os.path.join(results_dir, filename)
                if os.path.exists(filepath):
                    print(f"  {filename}")
                else:
                    print(f"  {filename} (not found)")
        else:
            print("Results directory not found")

        print("\n" + "=" * 80)

    def save_summary(
        self,
        summary: Dict[str, Any],
        filename: str = "results/test_execution_summary.json",
    ):
        
        import os

        os.makedirs("results", exist_ok=True)
        with open(filename, "w") as f:
            json.dump(summary, f, indent=2)
        print(f"Test execution summary saved to {filename}")


def main():
    
    runner = PerformanceTestRunner()

    # Run all tests
    summary = runner.run_all_tests()

    # Print summary
    runner.print_summary(summary)

    # Save summary
    runner.save_summary(summary)

    # Exit with appropriate code
    if summary["failed_tests"] > 0:
        print(f"\n  {summary['failed_tests']} test(s) failed!")
        sys.exit(1)
    else:
        print("\n All tests completed successfully!")
        sys.exit(0)


if __name__ == "__main__":
    main()
