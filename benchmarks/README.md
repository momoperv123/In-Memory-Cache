# Redis Clone Performance Testing Suite

This directory contains comprehensive performance testing and optimization tools for the Redis clone project.

## Overview

The performance testing suite includes:

- **Performance Benchmarks** - Measure operations per second and latency
- **Load Testing** - Test performance under concurrent load
- **Memory Profiling** - Analyze memory usage patterns
- **Latency Analysis** - Detailed latency measurement and analysis
- **Optimization Analysis** - Identify performance bottlenecks
- **Performance Comparison** - Compare before/after performance

## Quick Start

### 1. Start the Server
```bash
# In the project root directory
python main.py
```

### 2. Run All Tests
```bash
# In the benchmarks directory
python run_all_tests.py
```

### 3. Run Individual Tests
```bash
# Performance benchmarks
python performance_benchmark.py

# Load testing
python load_test.py

# Memory profiling
python memory_profiler.py

# Latency analysis
python latency_analyzer.py

# Optimization analysis
python optimization_analyzer.py

# Performance comparison
python performance_comparison.py
```

## Test Scripts

### `performance_benchmark.py`
Comprehensive performance benchmarks measuring:
- SET operations per second
- GET operations per second
- Mixed operations
- TTL operations
- Large value handling
- Concurrent client performance

**Output**: `benchmark_results.json`

### `load_test.py`
Load testing with multiple concurrent clients:
- Basic load testing
- Scaling tests (1, 2, 5, 10, 20, 50 workers)
- Sustained load testing
- Mixed operation load testing

**Output**: `load_test_results.json`

### `memory_profiler.py`
Memory usage analysis:
- Memory growth during operations
- Memory per operation
- Memory leak detection
- Concurrent memory usage
- Large value memory impact

**Output**: `memory_profile_results.json`

### `latency_analyzer.py`
Detailed latency analysis:
- Latency distribution
- Percentile analysis (P50, P95, P99, P99.9)
- Outlier detection
- Latency trends over time
- Value size impact on latency

**Output**: `latency_analysis_results.json`

### `optimization_analyzer.py`
Performance bottleneck analysis:
- CPU-bound issues
- Memory-bound issues
- Concurrency issues
- Algorithm inefficiencies
- Optimization recommendations

**Output**: `performance_analysis_report.json`

### `performance_comparison.py`
Before/after performance comparison:
- Benchmark comparison
- Load test comparison
- Memory comparison
- Latency comparison
- Overall improvement/regression analysis

**Output**: `performance_comparison_report.json`

## Dependencies

The performance testing suite requires additional Python packages:

```bash
pip install psutil numpy
```

## Understanding Results

### Performance Metrics

- **Operations per Second (OPS)**: Throughput measurement
- **Latency**: Time per operation (milliseconds)
- **Memory Growth**: Memory usage increase during operations
- **Success Rate**: Percentage of successful operations under load

### Key Percentiles

- **P50 (Median)**: 50% of operations complete within this time
- **P95**: 95% of operations complete within this time
- **P99**: 99% of operations complete within this time
- **P99.9**: 99.9% of operations complete within this time

### Performance Thresholds

- **Good Performance**: > 10,000 ops/sec, < 1ms latency
- **Acceptable Performance**: > 1,000 ops/sec, < 10ms latency
- **Poor Performance**: < 1,000 ops/sec, > 10ms latency

## Optimization Recommendations

The optimization analyzer provides recommendations in three categories:

### Immediate Priority
- Critical performance issues requiring immediate attention
- Memory leaks or excessive memory usage
- Algorithm inefficiencies

### Short-term Priority
- Concurrency improvements
- Connection pooling optimizations
- Locking mechanism improvements

### Long-term Priority
- Architectural improvements
- Feature additions
- Scalability enhancements

## Baseline Comparison

To establish performance baselines:

1. Run initial tests to create baseline
2. Make code changes
3. Run comparison tool to see improvements/regressions

```bash
# First run (creates baseline)
python run_all_tests.py

# After making changes
python performance_comparison.py
```

## Troubleshooting

### Common Issues

1. **Server not running**: Ensure `python main.py` is running
2. **Connection refused**: Check server port (default: 31337)
3. **Memory errors**: Reduce test sizes or increase system memory
4. **Timeout errors**: Increase timeout values in test scripts

### Performance Issues

1. **Low throughput**: Check for CPU bottlenecks
2. **High latency**: Look for algorithm inefficiencies
3. **Memory growth**: Investigate memory leaks
4. **Poor scaling**: Review concurrency implementation

## Customization

### Modifying Test Parameters

Edit the test scripts to adjust:
- Number of operations
- Number of concurrent clients
- Test durations
- Value sizes
- Sampling intervals

### Adding New Tests

1. Create new test script following existing patterns
2. Add to `run_all_tests.py` test list
3. Update this README with new test description

## Integration with CI/CD

The performance testing suite can be integrated into CI/CD pipelines:

```bash
# Run tests and fail on performance regressions
python run_all_tests.py
if [ $? -ne 0 ]; then
    echo "Performance tests failed"
    exit 1
fi
```

## Contributing

When adding new performance tests:

1. Follow existing code patterns
2. Include comprehensive error handling
3. Generate JSON output for analysis
4. Update documentation
5. Add to the master test runner

## License

This performance testing suite is part of the Redis clone project and follows the same license terms.
