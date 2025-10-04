import json
import os
import sys
import time
from typing import Any, Dict

# Add src to path
sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
)


class PerformanceComparison:
    def __init__(self):
        self.baseline_results = {}
        self.current_results = {}
        self.comparison_results = {}

    def load_baseline_results(
        self, baseline_file: str = "results/baseline_performance.json"
    ):
        
        if os.path.exists(baseline_file):
            with open(baseline_file) as f:
                self.baseline_results = json.load(f)
            print(f"Loaded baseline results from {baseline_file}")
        else:
            print(f"Warning: Baseline file {baseline_file} not found")

    def save_baseline_results(
        self, baseline_file: str = "results/baseline_performance.json"
    ):
        
        import os

        os.makedirs("results", exist_ok=True)
        with open(baseline_file, "w") as f:
            json.dump(self.current_results, f, indent=2)
        print(f"Saved baseline results to {baseline_file}")

    def load_current_results(self, results_dir: str = "results"):
        
        result_files = {
            "benchmark": "benchmark_results.json",
            "load_test": "load_test_results.json",
            "memory": "memory_profile_results.json",
            "latency": "latency_analysis_results.json",
        }

        for result_type, filename in result_files.items():
            filepath = os.path.join(results_dir, filename)
            if os.path.exists(filepath):
                try:
                    with open(filepath) as f:
                        if result_type not in self.current_results:
                            self.current_results[result_type] = {}
                        self.current_results[result_type] = json.load(f)
                    print(f"Loaded {result_type} results from {filename}")
                except Exception as e:
                    print(f"Error loading {filename}: {e}")
            else:
                print(f"Warning: {filename} not found")

    def compare_benchmark_results(self) -> Dict[str, Any]:
        
        if not self.baseline_results.get("benchmark") or not self.current_results.get(
            "benchmark"
        ):
            return {"error": "Missing benchmark results for comparison"}

        baseline = self.baseline_results["benchmark"]
        current = self.current_results["benchmark"]

        comparison = {
            "improvements": [],
            "regressions": [],
            "unchanged": [],
            "summary": {
                "total_tests": 0,
                "improvements": 0,
                "regressions": 0,
                "unchanged": 0,
            },
        }

        # Compare each test
        for test_name in baseline:
            if test_name not in current:
                continue

            baseline_result = baseline[test_name]
            current_result = current[test_name]

            if "error" in baseline_result or "error" in current_result:
                continue

            comparison["total_tests"] += 1

            # Compare key metrics
            baseline_ops = baseline_result.get("ops_per_second", 0)
            current_ops = current_result.get("ops_per_second", 0)

            baseline_latency = baseline_result.get("avg_latency_ms", 0)
            current_latency = current_result.get("avg_latency_ms", 0)

            # Calculate improvements
            ops_improvement = (
                ((current_ops - baseline_ops) / baseline_ops * 100)
                if baseline_ops > 0
                else 0
            )
            latency_improvement = (
                ((baseline_latency - current_latency) / baseline_latency * 100)
                if baseline_latency > 0
                else 0
            )

            # Determine if it's an improvement, regression, or unchanged
            if ops_improvement > 5 or latency_improvement > 5:  # 5% threshold
                comparison["improvements"].append(
                    {
                        "test": test_name,
                        "ops_per_second": {
                            "baseline": baseline_ops,
                            "current": current_ops,
                            "improvement": ops_improvement,
                        },
                        "latency": {
                            "baseline": baseline_latency,
                            "current": current_latency,
                            "improvement": latency_improvement,
                        },
                    }
                )
                comparison["summary"]["improvements"] += 1
            elif ops_improvement < -5 or latency_improvement < -5:  # 5% threshold
                comparison["regressions"].append(
                    {
                        "test": test_name,
                        "ops_per_second": {
                            "baseline": baseline_ops,
                            "current": current_ops,
                            "regression": ops_improvement,
                        },
                        "latency": {
                            "baseline": baseline_latency,
                            "current": current_latency,
                            "regression": latency_improvement,
                        },
                    }
                )
                comparison["summary"]["regressions"] += 1
            else:
                comparison["unchanged"].append(
                    {
                        "test": test_name,
                        "ops_per_second": {
                            "baseline": baseline_ops,
                            "current": current_ops,
                            "change": ops_improvement,
                        },
                        "latency": {
                            "baseline": baseline_latency,
                            "current": current_latency,
                            "change": latency_improvement,
                        },
                    }
                )
                comparison["summary"]["unchanged"] += 1

        return comparison

    def compare_load_test_results(self) -> Dict[str, Any]:
        
        if not self.baseline_results.get("load_test") or not self.current_results.get(
            "load_test"
        ):
            return {"error": "Missing load test results for comparison"}

        baseline = self.baseline_results["load_test"]
        current = self.current_results["load_test"]

        comparison = {
            "improvements": [],
            "regressions": [],
            "unchanged": [],
            "summary": {
                "total_tests": 0,
                "improvements": 0,
                "regressions": 0,
                "unchanged": 0,
            },
        }

        # Compare each test
        for test_name in baseline:
            if test_name not in current:
                continue

            baseline_result = baseline[test_name]
            current_result = current[test_name]

            if "error" in baseline_result or "error" in current_result:
                continue

            comparison["total_tests"] += 1

            # Compare key metrics
            baseline_ops = baseline_result.get("performance", {}).get(
                "overall_ops_per_second", 0
            )
            current_ops = current_result.get("performance", {}).get(
                "overall_ops_per_second", 0
            )

            baseline_success = baseline_result.get("performance", {}).get(
                "success_rate", 100
            )
            current_success = current_result.get("performance", {}).get(
                "success_rate", 100
            )

            # Calculate improvements
            ops_improvement = (
                ((current_ops - baseline_ops) / baseline_ops * 100)
                if baseline_ops > 0
                else 0
            )
            success_improvement = current_success - baseline_success

            # Determine if it's an improvement, regression, or unchanged
            if (
                ops_improvement > 5 or success_improvement > 1
            ):  # 5% ops threshold, 1% success threshold
                comparison["improvements"].append(
                    {
                        "test": test_name,
                        "ops_per_second": {
                            "baseline": baseline_ops,
                            "current": current_ops,
                            "improvement": ops_improvement,
                        },
                        "success_rate": {
                            "baseline": baseline_success,
                            "current": current_success,
                            "improvement": success_improvement,
                        },
                    }
                )
                comparison["summary"]["improvements"] += 1
            elif (
                ops_improvement < -5 or success_improvement < -1
            ):  # 5% ops threshold, 1% success threshold
                comparison["regressions"].append(
                    {
                        "test": test_name,
                        "ops_per_second": {
                            "baseline": baseline_ops,
                            "current": current_ops,
                            "regression": ops_improvement,
                        },
                        "success_rate": {
                            "baseline": baseline_success,
                            "current": current_success,
                            "regression": success_improvement,
                        },
                    }
                )
                comparison["summary"]["regressions"] += 1
            else:
                comparison["unchanged"].append(
                    {
                        "test": test_name,
                        "ops_per_second": {
                            "baseline": baseline_ops,
                            "current": current_ops,
                            "change": ops_improvement,
                        },
                        "success_rate": {
                            "baseline": baseline_success,
                            "current": current_success,
                            "change": success_improvement,
                        },
                    }
                )
                comparison["summary"]["unchanged"] += 1

        return comparison

    def compare_memory_results(self) -> Dict[str, Any]:
        
        if not self.baseline_results.get("memory") or not self.current_results.get(
            "memory"
        ):
            return {"error": "Missing memory results for comparison"}

        baseline = self.baseline_results["memory"]
        current = self.current_results["memory"]

        comparison = {
            "improvements": [],
            "regressions": [],
            "unchanged": [],
            "summary": {
                "total_tests": 0,
                "improvements": 0,
                "regressions": 0,
                "unchanged": 0,
            },
        }

        # Compare each test
        for test_name in baseline:
            if test_name not in current:
                continue

            baseline_result = baseline[test_name]
            current_result = current[test_name]

            if "error" in baseline_result or "error" in current_result:
                continue

            comparison["total_tests"] += 1

            # Compare key metrics
            baseline_memory = baseline_result.get("memory_growth_mb", 0)
            current_memory = current_result.get("memory_growth_mb", 0)

            baseline_per_op = baseline_result.get("memory_per_operation_kb", 0)
            current_per_op = current_result.get("memory_per_operation_kb", 0)

            # Calculate improvements (lower memory is better)
            memory_improvement = (
                ((baseline_memory - current_memory) / baseline_memory * 100)
                if baseline_memory > 0
                else 0
            )
            per_op_improvement = (
                ((baseline_per_op - current_per_op) / baseline_per_op * 100)
                if baseline_per_op > 0
                else 0
            )

            # Determine if it's an improvement, regression, or unchanged
            if memory_improvement > 5 or per_op_improvement > 5:  # 5% threshold
                comparison["improvements"].append(
                    {
                        "test": test_name,
                        "memory_growth": {
                            "baseline": baseline_memory,
                            "current": current_memory,
                            "improvement": memory_improvement,
                        },
                        "memory_per_operation": {
                            "baseline": baseline_per_op,
                            "current": current_per_op,
                            "improvement": per_op_improvement,
                        },
                    }
                )
                comparison["summary"]["improvements"] += 1
            elif memory_improvement < -5 or per_op_improvement < -5:  # 5% threshold
                comparison["regressions"].append(
                    {
                        "test": test_name,
                        "memory_growth": {
                            "baseline": baseline_memory,
                            "current": current_memory,
                            "regression": memory_improvement,
                        },
                        "memory_per_operation": {
                            "baseline": baseline_per_op,
                            "current": current_per_op,
                            "regression": per_op_improvement,
                        },
                    }
                )
                comparison["summary"]["regressions"] += 1
            else:
                comparison["unchanged"].append(
                    {
                        "test": test_name,
                        "memory_growth": {
                            "baseline": baseline_memory,
                            "current": current_memory,
                            "change": memory_improvement,
                        },
                        "memory_per_operation": {
                            "baseline": baseline_per_op,
                            "current": current_per_op,
                            "change": per_op_improvement,
                        },
                    }
                )
                comparison["summary"]["unchanged"] += 1

        return comparison

    def compare_latency_results(self) -> Dict[str, Any]:
        
        if not self.baseline_results.get("latency") or not self.current_results.get(
            "latency"
        ):
            return {"error": "Missing latency results for comparison"}

        baseline = self.baseline_results["latency"]
        current = self.current_results["latency"]

        comparison = {
            "improvements": [],
            "regressions": [],
            "unchanged": [],
            "summary": {
                "total_tests": 0,
                "improvements": 0,
                "regressions": 0,
                "unchanged": 0,
            },
        }

        # Compare each test
        for test_name in baseline:
            if test_name not in current:
                continue

            baseline_result = baseline[test_name]
            current_result = current[test_name]

            if "error" in baseline_result or "error" in current_result:
                continue

            comparison["total_tests"] += 1

            # Compare key metrics
            baseline_latency = baseline_result.get("statistics", {}).get(
                "avg_latency_ms", 0
            )
            current_latency = current_result.get("statistics", {}).get(
                "avg_latency_ms", 0
            )

            baseline_p95 = (
                baseline_result.get("statistics", {})
                .get("percentiles", {})
                .get("p95", 0)
            )
            current_p95 = (
                current_result.get("statistics", {})
                .get("percentiles", {})
                .get("p95", 0)
            )

            # Calculate improvements (lower latency is better)
            latency_improvement = (
                ((baseline_latency - current_latency) / baseline_latency * 100)
                if baseline_latency > 0
                else 0
            )
            p95_improvement = (
                ((baseline_p95 - current_p95) / baseline_p95 * 100)
                if baseline_p95 > 0
                else 0
            )

            # Determine if it's an improvement, regression, or unchanged
            if latency_improvement > 5 or p95_improvement > 5:  # 5% threshold
                comparison["improvements"].append(
                    {
                        "test": test_name,
                        "avg_latency": {
                            "baseline": baseline_latency,
                            "current": current_latency,
                            "improvement": latency_improvement,
                        },
                        "p95_latency": {
                            "baseline": baseline_p95,
                            "current": current_p95,
                            "improvement": p95_improvement,
                        },
                    }
                )
                comparison["summary"]["improvements"] += 1
            elif latency_improvement < -5 or p95_improvement < -5:  # 5% threshold
                comparison["regressions"].append(
                    {
                        "test": test_name,
                        "avg_latency": {
                            "baseline": baseline_latency,
                            "current": current_latency,
                            "regression": latency_improvement,
                        },
                        "p95_latency": {
                            "baseline": baseline_p95,
                            "current": current_p95,
                            "regression": p95_improvement,
                        },
                    }
                )
                comparison["summary"]["regressions"] += 1
            else:
                comparison["unchanged"].append(
                    {
                        "test": test_name,
                        "avg_latency": {
                            "baseline": baseline_latency,
                            "current": current_latency,
                            "change": latency_improvement,
                        },
                        "p95_latency": {
                            "baseline": baseline_p95,
                            "current": current_p95,
                            "change": p95_improvement,
                        },
                    }
                )
                comparison["summary"]["unchanged"] += 1

        return comparison

    def create_comprehensive_comparison(self) -> Dict[str, Any]:
        
        print("Creating comprehensive performance comparison...")

        comparison = {
            "timestamp": time.time(),
            "benchmark": self.compare_benchmark_results(),
            "load_test": self.compare_load_test_results(),
            "memory": self.compare_memory_results(),
            "latency": self.compare_latency_results(),
            "overall_summary": {},
        }

        # Calculate overall summary
        total_improvements = 0
        total_regressions = 0
        total_unchanged = 0
        total_tests = 0

        for category in ["benchmark", "load_test", "memory", "latency"]:
            if category in comparison and "summary" in comparison[category]:
                summary = comparison[category]["summary"]
                total_improvements += summary.get("improvements", 0)
                total_regressions += summary.get("regressions", 0)
                total_unchanged += summary.get("unchanged", 0)
                total_tests += summary.get("total_tests", 0)

        comparison["overall_summary"] = {
            "total_tests": total_tests,
            "total_improvements": total_improvements,
            "total_regressions": total_regressions,
            "total_unchanged": total_unchanged,
            "improvement_rate": (total_improvements / total_tests * 100)
            if total_tests > 0
            else 0,
            "regression_rate": (total_regressions / total_tests * 100)
            if total_tests > 0
            else 0,
        }

        return comparison

    def print_comparison_report(self, comparison: Dict[str, Any]):
        
        print("\n" + "=" * 80)
        print("REDIS CLONE PERFORMANCE COMPARISON REPORT")
        print("=" * 80)

        # Overall summary
        print("\nOVERALL SUMMARY")
        print("-" * 40)
        overall = comparison.get("overall_summary", {})
        print(f"Total Tests: {overall.get('total_tests', 0)}")
        print(
            f"Improvements: {overall.get('total_improvements', 0)} ({overall.get('improvement_rate', 0):.1f}%)"
        )
        print(
            f"Regressions: {overall.get('total_regressions', 0)} ({overall.get('regression_rate', 0):.1f}%)"
        )
        print(f"Unchanged: {overall.get('total_unchanged', 0)}")

        # Category comparisons
        categories = ["benchmark", "load_test", "memory", "latency"]
        category_names = {
            "benchmark": "Benchmark Tests",
            "load_test": "Load Tests",
            "memory": "Memory Tests",
            "latency": "Latency Tests",
        }

        for category in categories:
            if category in comparison and "error" not in comparison[category]:
                print(f"\n{category_names[category].upper()}")
                print("-" * 40)

                cat_data = comparison[category]
                summary = cat_data.get("summary", {})

                print(f"Tests: {summary.get('total_tests', 0)}")
                print(f"Improvements: {summary.get('improvements', 0)}")
                print(f"Regressions: {summary.get('regressions', 0)}")
                print(f"Unchanged: {summary.get('unchanged', 0)}")

                # Show key improvements
                if cat_data.get("improvements"):
                    print("\nKey Improvements:")
                    for improvement in cat_data["improvements"][:3]:  # Show top 3
                        test_name = improvement["test"]
                        print(f"  • {test_name}")

                # Show key regressions
                if cat_data.get("regressions"):
                    print("\nKey Regressions:")
                    for regression in cat_data["regressions"][:3]:  # Show top 3
                        test_name = regression["test"]
                        print(f"  • {test_name}")

        print("\n" + "=" * 80)


def main():
    
    comparison_tool = PerformanceComparison()

    # Load current results
    comparison_tool.load_current_results()

    # Load baseline results
    comparison_tool.load_baseline_results()

    # Create comparison
    comparison = comparison_tool.create_comprehensive_comparison()

    # Print report
    comparison_tool.print_comparison_report(comparison)

    # Save comparison
    import os

    # Create results directory if it doesn't exist
    results_dir = "results"
    os.makedirs(results_dir, exist_ok=True)

    with open(
        os.path.join(results_dir, "performance_comparison_report.json"), "w"
    ) as f:
        json.dump(comparison, f, indent=2)

    print(
        f"\nComparison report saved to {results_dir}/performance_comparison_report.json"
    )

    # Option to save current results as new baseline
    save_baseline = (
        input("\nSave current results as new baseline? (y/n): ").lower().strip()
    )
    if save_baseline == "y":
        comparison_tool.save_baseline_results()
        print("Current results saved as new baseline")


if __name__ == "__main__":
    main()
