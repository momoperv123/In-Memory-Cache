#!/usr/bin/env python3
"""
Optimization analyzer for Redis clone
Analyzes performance results and suggests optimizations
"""

import json
import os
import statistics
import sys
from typing import Any, Dict, List

# Add src to path
sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
)


class OptimizationAnalyzer:
    def __init__(self):
        self.benchmark_results = {}
        self.load_test_results = {}
        self.memory_results = {}
        self.latency_results = {}

    def load_results(self, results_dir: str = "results"):
        """Load all performance test results"""
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
                        if result_type == "benchmark":
                            self.benchmark_results = json.load(f)
                        elif result_type == "load_test":
                            self.load_test_results = json.load(f)
                        elif result_type == "memory":
                            self.memory_results = json.load(f)
                        elif result_type == "latency":
                            self.latency_results = json.load(f)
                    print(f"Loaded {result_type} results from {filename}")
                except Exception as e:
                    print(f"Error loading {filename}: {e}")
            else:
                print(f"Warning: {filename} not found")

    def analyze_performance_bottlenecks(self) -> Dict[str, Any]:
        """Analyze performance bottlenecks from test results"""
        print("Analyzing performance bottlenecks...")

        bottlenecks = {
            "cpu_bound": [],
            "memory_bound": [],
            "network_bound": [],
            "concurrency_issues": [],
            "algorithm_inefficiencies": [],
        }

        # Analyze benchmark results
        if self.benchmark_results:
            bottlenecks.update(self._analyze_benchmark_bottlenecks())

        # Analyze load test results
        if self.load_test_results:
            bottlenecks.update(self._analyze_load_test_bottlenecks())

        # Analyze memory results
        if self.memory_results:
            bottlenecks.update(self._analyze_memory_bottlenecks())

        # Analyze latency results
        if self.latency_results:
            bottlenecks.update(self._analyze_latency_bottlenecks())

        return bottlenecks

    def _analyze_benchmark_bottlenecks(self) -> Dict[str, List[str]]:
        """Analyze bottlenecks from benchmark results"""
        bottlenecks = {
            "cpu_bound": [],
            "algorithm_inefficiencies": [],
        }

        # Check for low operations per second
        for test_name, result in self.benchmark_results.items():
            if "error" in result:
                continue

            ops_per_sec = result.get("ops_per_second", 0)
            avg_latency = result.get("avg_latency_ms", 0)

            # Low throughput indicators
            if ops_per_sec < 1000:
                bottlenecks["cpu_bound"].append(
                    f"{test_name}: Very low throughput ({ops_per_sec:.2f} ops/sec)"
                )

            # High latency indicators
            if avg_latency > 10:
                bottlenecks["algorithm_inefficiencies"].append(
                    f"{test_name}: High latency ({avg_latency:.3f}ms)"
                )

            # Check for performance degradation with large values
            if "Large Values" in test_name and ops_per_sec < 100:
                bottlenecks["algorithm_inefficiencies"].append(
                    f"{test_name}: Poor performance with large values"
                )

        return bottlenecks

    def _analyze_load_test_bottlenecks(self) -> Dict[str, List[str]]:
        """Analyze bottlenecks from load test results"""
        bottlenecks = {
            "concurrency_issues": [],
            "network_bound": [],
        }

        # Analyze scaling results
        if "scaling" in self.load_test_results:
            scaling = self.load_test_results["scaling"]

            # Check for poor scaling
            worker_counts = []
            ops_per_sec = []

            for _, result in scaling.items():
                if "error" in result:
                    continue

                config = result.get("test_config", {})
                performance = result.get("performance", {})

                if "num_workers" in config and "overall_ops_per_second" in performance:
                    worker_counts.append(config["num_workers"])
                    ops_per_sec.append(performance["overall_ops_per_second"])

            # Check for scaling issues
            if len(ops_per_sec) > 1:
                # Calculate scaling efficiency
                base_ops = ops_per_sec[0] if ops_per_sec else 0
                max_ops = max(ops_per_sec) if ops_per_sec else 0

                if max_ops < base_ops * 2:  # Less than 2x improvement with more workers
                    bottlenecks["concurrency_issues"].append(
                        "Poor scaling with increased concurrency"
                    )

                # Check for performance degradation
                if max(ops_per_sec) < base_ops:
                    bottlenecks["concurrency_issues"].append(
                        "Performance degrades with more workers"
                    )

        # Analyze sustained load results
        if "sustained" in self.load_test_results:
            sustained = self.load_test_results["sustained"]
            performance = sustained.get("performance", {})

            success_rate = performance.get("success_rate", 100)
            if success_rate < 95:
                bottlenecks["network_bound"].append(
                    f"Low success rate under sustained load: {success_rate:.1f}%"
                )

        return bottlenecks

    def _analyze_memory_bottlenecks(self) -> Dict[str, List[str]]:
        """Analyze bottlenecks from memory results"""
        bottlenecks = {
            "memory_bound": [],
        }

        for test_name, result in self.memory_results.items():
            if "error" in result:
                continue

            # Check for high memory growth
            memory_growth = result.get("memory_growth_mb", 0)
            num_operations = result.get("num_operations", 1)

            if memory_growth > 0:
                memory_per_op = (
                    memory_growth * 1024
                ) / num_operations  # KB per operation

                if memory_per_op > 1:  # More than 1KB per operation
                    bottlenecks["memory_bound"].append(
                        f"{test_name}: High memory per operation ({memory_per_op:.2f}KB)"
                    )

                if memory_growth > 100:  # More than 100MB growth
                    bottlenecks["memory_bound"].append(
                        f"{test_name}: Excessive memory growth ({memory_growth:.2f}MB)"
                    )

            # Check for memory leaks (high growth after GC)
            memory_after_gc = result.get("memory_growth_after_gc_mb", 0)
            if memory_after_gc > memory_growth * 0.8:  # Most memory not freed by GC
                bottlenecks["memory_bound"].append(
                    f"{test_name}: Potential memory leak"
                )

        return bottlenecks

    def _analyze_latency_bottlenecks(self) -> Dict[str, List[str]]:
        """Analyze bottlenecks from latency results"""
        bottlenecks = {
            "algorithm_inefficiencies": [],
        }

        for test_name, result in self.latency_results.items():
            if "error" in result:
                continue

            # Check for high latency variance
            if "statistics" in result:
                stats = result["statistics"]
                std_dev = stats.get("std_dev_ms", 0)
                avg_latency = stats.get("avg_latency_ms", 0)

                if std_dev > avg_latency * 0.5:  # High variance
                    bottlenecks["algorithm_inefficiencies"].append(
                        f"{test_name}: High latency variance"
                    )

            # Check for outliers
            if "statistics" in result:
                stats = result["statistics"]
                outlier_percentage = stats.get("outlier_percentage", 0)

                if outlier_percentage > 5:  # More than 5% outliers
                    bottlenecks["algorithm_inefficiencies"].append(
                        f"{test_name}: High outlier rate ({outlier_percentage:.1f}%)"
                    )

        return bottlenecks

    def generate_optimization_recommendations(self) -> Dict[str, List[str]]:
        """Generate optimization recommendations based on analysis"""
        print("Generating optimization recommendations...")

        recommendations = {
            "immediate": [],
            "short_term": [],
            "long_term": [],
        }

        # Analyze bottlenecks
        bottlenecks = self.analyze_performance_bottlenecks()

        # Generate recommendations based on bottlenecks
        if bottlenecks["cpu_bound"]:
            recommendations["immediate"].extend(
                [
                    "Optimize hot code paths in command processing",
                    "Consider using more efficient data structures",
                    "Profile and optimize the most frequently called functions",
                ]
            )

        if bottlenecks["memory_bound"]:
            recommendations["immediate"].extend(
                [
                    "Implement object pooling for frequently created objects",
                    "Optimize memory allocation patterns",
                    "Consider using more memory-efficient data structures",
                ]
            )

        if bottlenecks["concurrency_issues"]:
            recommendations["short_term"].extend(
                [
                    "Review and optimize locking mechanisms",
                    "Consider using lock-free data structures where possible",
                    "Implement better connection pooling",
                ]
            )

        if bottlenecks["algorithm_inefficiencies"]:
            recommendations["immediate"].extend(
                [
                    "Optimize key lookup algorithms",
                    "Implement caching for frequently accessed data",
                    "Review and optimize TTL expiration logic",
                ]
            )

        # General recommendations based on performance patterns
        if self.benchmark_results:
            recommendations["long_term"].extend(
                [
                    "Consider implementing Redis-compatible persistence",
                    "Add support for more Redis data types (lists, sets, hashes)",
                    "Implement Redis-compatible replication",
                ]
            )

        return recommendations

    def create_performance_report(self) -> Dict[str, Any]:
        """Create comprehensive performance report"""
        print("Creating performance report...")

        report = {
            "summary": {},
            "bottlenecks": {},
            "recommendations": {},
            "detailed_results": {},
        }

        # Summary statistics
        if self.benchmark_results:
            report["summary"]["benchmark_results"] = self._summarize_benchmark_results()

        if self.load_test_results:
            report["summary"]["load_test_results"] = self._summarize_load_test_results()

        if self.memory_results:
            report["summary"]["memory_results"] = self._summarize_memory_results()

        if self.latency_results:
            report["summary"]["latency_results"] = self._summarize_latency_results()

        # Bottlenecks analysis
        report["bottlenecks"] = self.analyze_performance_bottlenecks()

        # Optimization recommendations
        report["recommendations"] = self.generate_optimization_recommendations()

        # Detailed results
        report["detailed_results"] = {
            "benchmark": self.benchmark_results,
            "load_test": self.load_test_results,
            "memory": self.memory_results,
            "latency": self.latency_results,
        }

        return report

    def _summarize_benchmark_results(self) -> Dict[str, Any]:
        """Summarize benchmark results"""
        summary = {
            "total_tests": len(self.benchmark_results),
            "successful_tests": 0,
            "failed_tests": 0,
            "avg_ops_per_second": 0,
            "avg_latency_ms": 0,
        }

        ops_per_sec = []
        latencies = []

        for _, result in self.benchmark_results.items():
            if "error" in result:
                summary["failed_tests"] += 1
            else:
                summary["successful_tests"] += 1
                if "ops_per_second" in result:
                    ops_per_sec.append(result["ops_per_second"])
                if "avg_latency_ms" in result:
                    latencies.append(result["avg_latency_ms"])

        if ops_per_sec:
            summary["avg_ops_per_second"] = statistics.mean(ops_per_sec)
        if latencies:
            summary["avg_latency_ms"] = statistics.mean(latencies)

        return summary

    def _summarize_load_test_results(self) -> Dict[str, Any]:
        """Summarize load test results"""
        summary = {
            "total_tests": len(self.load_test_results),
            "successful_tests": 0,
            "failed_tests": 0,
            "max_concurrent_ops_per_second": 0,
            "avg_success_rate": 0,
        }

        success_rates = []
        max_ops = 0

        for _, result in self.load_test_results.items():
            if "error" in result:
                summary["failed_tests"] += 1
            else:
                summary["successful_tests"] += 1

                if "performance" in result:
                    perf = result["performance"]
                    if "overall_ops_per_second" in perf:
                        max_ops = max(max_ops, perf["overall_ops_per_second"])
                    if "success_rate" in perf:
                        success_rates.append(perf["success_rate"])

        summary["max_concurrent_ops_per_second"] = max_ops
        if success_rates:
            summary["avg_success_rate"] = statistics.mean(success_rates)

        return summary

    def _summarize_memory_results(self) -> Dict[str, Any]:
        """Summarize memory results"""
        summary = {
            "total_tests": len(self.memory_results),
            "successful_tests": 0,
            "failed_tests": 0,
            "max_memory_growth_mb": 0,
            "avg_memory_per_operation_kb": 0,
        }

        memory_growths = []
        memory_per_ops = []

        for _, result in self.memory_results.items():
            if "error" in result:
                summary["failed_tests"] += 1
            else:
                summary["successful_tests"] += 1

                if "memory_growth_mb" in result:
                    memory_growths.append(result["memory_growth_mb"])
                    summary["max_memory_growth_mb"] = max(
                        summary["max_memory_growth_mb"], result["memory_growth_mb"]
                    )

                if "memory_per_operation_kb" in result:
                    memory_per_ops.append(result["memory_per_operation_kb"])

        if memory_per_ops:
            summary["avg_memory_per_operation_kb"] = statistics.mean(memory_per_ops)

        return summary

    def _summarize_latency_results(self) -> Dict[str, Any]:
        """Summarize latency results"""
        summary = {
            "total_tests": len(self.latency_results),
            "successful_tests": 0,
            "failed_tests": 0,
            "avg_latency_ms": 0,
            "max_latency_ms": 0,
        }

        latencies = []
        max_latency = 0

        for _, result in self.latency_results.items():
            if "error" in result:
                summary["failed_tests"] += 1
            else:
                summary["successful_tests"] += 1

                if "statistics" in result:
                    stats = result["statistics"]
                    if "avg_latency_ms" in stats:
                        latencies.append(stats["avg_latency_ms"])
                    if "max_latency_ms" in stats:
                        max_latency = max(max_latency, stats["max_latency_ms"])

        if latencies:
            summary["avg_latency_ms"] = statistics.mean(latencies)
        summary["max_latency_ms"] = max_latency

        return summary

    def print_report(self, report: Dict[str, Any]):
        """Print formatted performance report"""
        print("\n" + "=" * 80)
        print("REDIS CLONE PERFORMANCE ANALYSIS REPORT")
        print("=" * 80)

        # Summary
        print("\n SUMMARY")
        print("-" * 40)
        summary = report.get("summary", {})

        for category, data in summary.items():
            print(f"\n{category.upper()}:")
            for key, value in data.items():
                if isinstance(value, float):
                    print(f"  {key}: {value:.2f}")
                else:
                    print(f"  {key}: {value}")

        # Bottlenecks
        print("\n PERFORMANCE BOTTLENECKS")
        print("-" * 40)
        bottlenecks = report.get("bottlenecks", {})

        for category, issues in bottlenecks.items():
            if issues:
                print(f"\n{category.upper()}:")
                for issue in issues:
                    print(f"  • {issue}")

        # Recommendations
        print("\n OPTIMIZATION RECOMMENDATIONS")
        print("-" * 40)
        recommendations = report.get("recommendations", {})

        for priority, recs in recommendations.items():
            if recs:
                print(f"\n{priority.upper()} PRIORITY:")
                for rec in recs:
                    print(f"  • {rec}")

        print("\n" + "=" * 80)


def main():
    """Main optimization analyzer runner"""
    analyzer = OptimizationAnalyzer()

    # Load results
    analyzer.load_results()

    # Create and print report
    report = analyzer.create_performance_report()
    analyzer.print_report(report)

    # Save report
    import os

    # Create results directory if it doesn't exist
    results_dir = "results"
    os.makedirs(results_dir, exist_ok=True)

    with open(os.path.join(results_dir, "performance_analysis_report.json"), "w") as f:
        json.dump(report, f, indent=2)

    print(f"\nReport saved to {results_dir}/performance_analysis_report.json")


if __name__ == "__main__":
    main()
