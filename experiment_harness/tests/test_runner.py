"""Tests for the experiment harness."""

import pytest
import tempfile
import os
from pathlib import Path

from experiment_harness import (
    ExperimentStrategy,
    ExperimentContext,
    ExperimentConfig,
    ExperimentRunner,
    ExperimentResult,
    time_execution,
)


class SimpleExperiment(ExperimentStrategy):
    """Simple test experiment."""
    
    def setup(self, context):
        self.setup_called = True
        context.shared_state['counter'] = 0
    
    def execute(self, context):
        context.shared_state['counter'] += 1
        return ExperimentResult(
            duration_ms=10.5,
            custom_metrics={
                'counter': context.shared_state['counter'],
                'execution_num': context.execution_number,
            }
        )
    
    def teardown(self, context):
        self.teardown_called = True
    
    def get_metrics(self):
        return ['counter', 'execution_num']


class FailingExperiment(ExperimentStrategy):
    """Experiment that fails on some executions."""
    
    def setup(self, context):
        pass
    
    def execute(self, context):
        if context.execution_number == 2:
            raise ValueError("Intentional failure")
        return ExperimentResult(duration_ms=5.0)
    
    def teardown(self, context):
        pass


class DatabaseExperiment(ExperimentStrategy):
    """Experiment that uses database connection."""
    
    def setup(self, context):
        if context.database_connection:
            context.database_connection.execute("CREATE TABLE test (id INTEGER)")
            context.database_connection.execute("INSERT INTO test VALUES (1), (2), (3)")
    
    def execute(self, context):
        if context.database_connection:
            result = context.database_connection.execute("SELECT COUNT(*) FROM test").fetchone()
            return ExperimentResult(
                duration_ms=1.0,
                custom_metrics={'row_count': result[0] if result else 0}
            )
        return ExperimentResult(duration_ms=0.0)
    
    def teardown(self, context):
        if context.database_connection:
            context.database_connection.execute("DROP TABLE IF EXISTS test")


def test_simple_experiment():
    """Test basic experiment execution."""
    strategy = SimpleExperiment()
    config = ExperimentConfig(
        num_executions=3,
        num_warmup_runs=1,
        verbose=False,
    )
    
    runner = ExperimentRunner(strategy, config)
    collector = runner.run()
    
    assert len(collector.results) == 3
    assert all(r.duration_ms == 10.5 for r in collector.results)
    # Counter starts at 1 for first collected result (warm-up run incremented it)
    assert collector.results[0].custom_metrics['counter'] == 2
    assert collector.results[1].custom_metrics['counter'] == 3
    assert collector.results[2].custom_metrics['counter'] == 4
    assert strategy.setup_called
    assert strategy.teardown_called


def test_warmup_runs():
    """Test that warm-up runs are executed but not collected."""
    strategy = SimpleExperiment()
    config = ExperimentConfig(
        num_executions=3,
        num_warmup_runs=2,
        verbose=False,
    )
    
    runner = ExperimentRunner(strategy, config)
    collector = runner.run()
    
    # Should have 3 results (warm-up runs discarded, but counter continues)
    assert len(collector.results) == 3
    # Counter should start at 3 for first collected result (2 warm-ups incremented it)
    assert collector.results[0].custom_metrics['counter'] == 3


def test_failing_experiment():
    """Test experiment with failures."""
    strategy = FailingExperiment()
    config = ExperimentConfig(
        num_executions=3,
        verbose=False,
    )
    
    runner = ExperimentRunner(strategy, config)
    collector = runner.run()
    
    assert len(collector.results) == 3
    assert collector.results[0].error is None
    assert collector.results[1].error is not None
    assert "Intentional failure" in collector.results[1].error
    assert collector.results[2].error is None


def test_csv_export():
    """Test CSV export functionality."""
    with tempfile.TemporaryDirectory() as tmpdir:
        strategy = SimpleExperiment()
        config = ExperimentConfig(
            num_executions=3,
            output_dir=tmpdir,
            output_filename="test_results.csv",
            verbose=False,
        )
        
        runner = ExperimentRunner(strategy, config)
        collector = runner.run()
        
        csv_path = Path(tmpdir) / "test_results.csv"
        assert csv_path.exists()
        
        # Read and verify CSV
        with open(csv_path, 'r') as f:
            lines = f.readlines()
            assert len(lines) == 4  # Header + 3 results (no summary row)
            assert 'execution_number' in lines[0]
            assert 'duration_ms' in lines[0]
            assert 'counter' in lines[0]
            assert 'execution_num' in lines[0]


def test_database_experiment():
    """Test experiment with database connection."""
    strategy = DatabaseExperiment()
    config = ExperimentConfig(
        num_executions=2,
        database_config={
            "database": ":memory:",
        },
        verbose=False,
    )
    
    runner = ExperimentRunner(strategy, config)
    collector = runner.run()
    
    assert len(collector.results) == 2
    assert all(r.custom_metrics.get('row_count') == 3 for r in collector.results)


def test_system_config():
    """Test system configuration application."""
    strategy = SimpleExperiment()
    config = ExperimentConfig(
        num_executions=1,
        database_config={
            "database": ":memory:",
        },
        system_config={
            "threads": 2,
        },
        verbose=False,
    )
    
    runner = ExperimentRunner(strategy, config)
    collector = runner.run()
    
    # Verify system config was applied (no exception means it worked)
    assert len(collector.results) == 1


def test_setup_teardown_steps():
    """Test setup and teardown steps."""
    setup_called = []
    teardown_called = []
    
    def setup_step():
        setup_called.append(True)
    
    def teardown_step():
        teardown_called.append(True)
    
    strategy = SimpleExperiment()
    config = ExperimentConfig(
        num_executions=1,
        setup_steps=[setup_step],
        teardown_steps=[teardown_step],
        verbose=False,
    )
    
    runner = ExperimentRunner(strategy, config)
    runner.run()
    
    assert len(setup_called) == 1
    assert len(teardown_called) == 1


def test_time_execution_context_manager():
    """Test time_execution context manager."""
    with time_execution() as timing:
        import time
        time.sleep(0.01)  # Sleep for 10ms
    
    assert 'duration_ms' in timing
    assert timing['duration_ms'] >= 10.0
    assert timing['duration_ms'] < 100.0  # Should be close to 10ms


def test_config_validation():
    """Test configuration validation."""
    # Invalid num_executions
    with pytest.raises(ValueError, match="num_executions must be at least 1"):
        ExperimentConfig(num_executions=0)
    
    # Invalid num_warmup_runs
    with pytest.raises(ValueError, match="num_warmup_runs must be non-negative"):
        ExperimentConfig(num_warmup_runs=-1)
    
    # num_warmup_runs >= num_executions
    with pytest.raises(ValueError, match="num_warmup_runs must be less than num_executions"):
        ExperimentConfig(num_executions=5, num_warmup_runs=5)


def test_result_collector_summary():
    """Test result collector summary calculation."""
    from experiment_harness import ResultCollector
    
    collector = ResultCollector("./test", "test.csv")
    
    # Add some results
    for i in range(5):
        collector.add_result(ExperimentResult(
            duration_ms=10.0 + i,
            custom_metrics={'value': i * 2}
        ))
    
    summary = collector._calculate_summary(['value'])
    assert 'mean=' in summary['duration_ms']
    assert 'mean=' in summary['value']


def test_context_shared_state():
    """Test that shared state persists across executions."""
    class StateExperiment(ExperimentStrategy):
        def setup(self, context):
            context.shared_state['accumulator'] = 0
        
        def execute(self, context):
            context.shared_state['accumulator'] += context.execution_number
            return ExperimentResult(
                duration_ms=1.0,
                custom_metrics={'accumulator': context.shared_state['accumulator']}
            )
        
        def teardown(self, context):
            pass
    
    strategy = StateExperiment()
    config = ExperimentConfig(num_executions=3, verbose=False)
    
    runner = ExperimentRunner(strategy, config)
    collector = runner.run()
    
    # Accumulator should be 1+2+3 = 6 by the end
    assert collector.results[2].custom_metrics['accumulator'] == 6
