# Experiment Harness

A reusable framework for running experiments using the Strategy design pattern. This project provides a flexible infrastructure for executing experiments with configurable parameters, warm-up runs, setup/teardown steps, and result collection.

## Features

- **Strategy Pattern**: Define experiments by implementing the `ExperimentStrategy` interface
- **Configurable Execution**: Control number of runs, warm-up runs, and execution parameters
- **Database Support**: Built-in support for DuckDB connections with configurable parameters
- **Metrics Collection**: Collect timing and custom metrics from experiments
- **CSV Export**: Automatically export results to CSV files with summary statistics
- **Lifecycle Management**: Automatic setup, execution, and teardown handling

## Installation

This project uses `uv` for dependency management. To install:

```bash
cd experiment_harness
uv sync
```

## Linting and Tests

Run from the `experiment_harness` directory.

```bash
python3 -m ruff check .
uv run --group dev python -m pytest
```

## Quick Start

### Define an Experiment Strategy

```python
from experiment_harness import ExperimentStrategy, ExperimentContext, ExperimentResult

class MyExperiment(ExperimentStrategy):
    def setup(self, context):
        """One-time setup before all runs."""
        # Initialize resources, create tables, etc.
        if context.database_connection:
            context.database_connection.execute("CREATE TABLE test (id INTEGER)")
    
    def execute(self, context):
        """Execute a single experiment run."""
        # Run your experiment code
        import time
        start = time.perf_counter()
        
        # Your experiment logic here
        result = context.database_connection.execute("SELECT COUNT(*) FROM test").fetchone()
        
        duration = (time.perf_counter() - start) * 1000.0
        
        return ExperimentResult(
            duration_ms=duration,
            custom_metrics={
                "rows_processed": result[0] if result else 0,
            }
        )
    
    def teardown(self, context):
        """Cleanup after all runs."""
        if context.database_connection:
            context.database_connection.execute("DROP TABLE IF EXISTS test")
```

### Run the Experiment

```python
from experiment_harness import ExperimentRunner, ExperimentConfig

config = ExperimentConfig(
    num_executions=10,
    num_warmup_runs=2,
    database_config={
        "database": ":memory:",
        "config": {"allow_unsigned_extensions": "true"},
    },
    system_config={
        "threads": 4,
    },
    output_dir="./results",
    output_filename="my_experiment.csv",
    verbose=True,
)

runner = ExperimentRunner(MyExperiment(), config)
collector = runner.run()
```

## Configuration

### ExperimentConfig

The `ExperimentConfig` class supports the following parameters:

- `num_executions` (int): Number of experiment runs (default: 1)
- `num_warmup_runs` (int): Number of warm-up runs to discard (default: 0)
- `setup_steps` (List[Callable]): Optional setup functions to run before experiment
- `teardown_steps` (List[Callable]): Optional teardown functions to run after experiment
- `database_config` (Dict): Database connection parameters (see below)
- `system_config` (Dict): System-level parameters (threads, memory, etc.)
- `output_dir` (str): Directory for CSV output (default: "./results")
- `output_filename` (str): Base filename for CSV output (default: "results.csv")
- `verbose` (bool): Enable verbose logging (default: False)

### Database Configuration

The `database_config` dictionary supports:

- `database` (str): Database path or ":memory:" for in-memory (default: ":memory:")
- `config` (Dict): DuckDB configuration dictionary
- `extensions` (List): List of extensions to load. Can be:
  - List of strings: `["extension_name"]`
  - List of dicts: `[{"name": "ext_name", "path": "optional/path/to/ext"}]`

Example:

```python
database_config = {
    "database": ":memory:",
    "config": {
        "allow_unsigned_extensions": "true",
    },
    "extensions": [
        "extension_name",
        {"name": "custom_ext", "path": "/path/to/custom.duckdb_extension"},
    ],
}
```

### System Configuration

The `system_config` dictionary contains key-value pairs that are applied as DuckDB SET commands:

```python
system_config = {
    "threads": 4,
    "memory_limit": "1GB",
}
```

## Results

### ExperimentResult

Each execution returns an `ExperimentResult` with:

- `duration_ms` (float): Execution duration in milliseconds
- `custom_metrics` (Dict): Dictionary of custom metric names to values
- `timestamp` (datetime): Timestamp when result was collected
- `error` (str, optional): Error message if execution failed

### CSV Export

Results are automatically exported to CSV with:

- One row per execution
- Columns: `execution_number`, `timestamp`, `duration_ms`, `error`, plus all custom metrics
- Summary statistics row with mean, median, stddev, min, max for numeric metrics

### Console Output

When `verbose=True`, the runner prints:
- Setup/teardown step execution
- Progress for each execution
- Summary statistics after completion

## Metrics Utilities

The package provides utilities for common metrics:

### Timing

```python
from experiment_harness import time_execution

with time_execution() as timing:
    # Your code here
    result = some_operation()
duration_ms = timing['duration_ms']
```

### Memory Usage

```python
from experiment_harness import collect_memory_usage

metrics = collect_memory_usage()
# Returns {'memory_mb': 123.45} if psutil is available
```

## Advanced Usage

### Using Setup/Teardown Steps

```python
def my_setup():
    print("Setting up external resources...")

def my_teardown():
    print("Cleaning up external resources...")

config = ExperimentConfig(
    num_executions=5,
    setup_steps=[my_setup],
    teardown_steps=[my_teardown],
)
```

### Accessing Context in Strategies

The `ExperimentContext` provides:

- `execution_number` (int): Current execution number (1-indexed)
- `database_connection`: Database connection if configured
- `system_config` (Dict): System configuration
- `shared_state` (Dict): Dictionary for sharing state between executions

```python
def execute(self, context):
    # Access current execution number
    if context.execution_number == 1:
        # First execution special handling
        pass
    
    # Store state for next execution
    context.shared_state['last_result'] = some_value
    
    # Access previous state
    prev_result = context.shared_state.get('last_result')
```

## Testing

Run tests with:

```bash
uv run pytest
```

## Project Structure

```
experiment_harness/
├── src/
│   └── experiment_harness/
│       ├── __init__.py      # Package exports
│       ├── strategy.py       # ExperimentStrategy interface
│       ├── config.py         # Configuration classes
│       ├── runner.py         # ExperimentRunner
│       ├── results.py        # Result collection and CSV export
│       └── metrics.py        # Metrics utilities
├── tests/
│   └── test_runner.py        # Unit tests
└── pyproject.toml            # Project configuration
```

## Integration with Other Projects

Experiments can be defined in other directories and use this harness. For example:

```python
# In another project directory
from experiment_harness import ExperimentStrategy, ExperimentRunner, ExperimentConfig
from sql_rewriter import SQLRewriter

class SQLRewriterExperiment(ExperimentStrategy):
    def setup(self, context):
        self.rewriter = SQLRewriter(conn=context.database_connection)
        # ... setup policies, etc.
    
    def execute(self, context):
        # Run experiment with SQLRewriter
        # ...
```

## License

See the main project LICENSE file.
