"""Experiment runner that executes strategies and manages lifecycle."""

from typing import Optional

import duckdb

from .config import ExperimentConfig
from .metrics import time_execution
from .results import ExperimentResult, ResultCollector
from .strategy import ExperimentContext, ExperimentStrategy


class ExperimentRunner:
    """Runs experiments using a strategy and configuration."""

    def __init__(self, strategy: ExperimentStrategy, config: ExperimentConfig):
        """Initialize experiment runner.

        Args:
            strategy: Experiment strategy to execute
            config: Experiment configuration
        """
        self.strategy = strategy
        self.config = config
        self.collector = ResultCollector(config.output_dir, config.output_filename)
        self.context: Optional[ExperimentContext] = None

    def run(self) -> ResultCollector:
        """Run the experiment according to configuration.

        Returns:
            ResultCollector with all collected results
        """
        if self.config.verbose:
            print(f"Starting experiment: {self.config.num_executions} executions, {self.config.num_warmup_runs} warm-up runs")

        try:
            # Run setup steps
            for step in self.config.setup_steps:
                if self.config.verbose:
                    print(f"Running setup step: {step.__name__}")
                step()

            # Initialize database connection if configured
            db_conn = None
            if self.config.database_config:
                db_conn = self._create_database_connection()

            # Create context
            self.context = ExperimentContext(
                database_connection=db_conn,
                strategy_config=self.config.strategy_config or {}
            )

            # Apply system configuration
            if db_conn and self.config.db_settings:
                self._apply_db_settings(db_conn)

            # Call strategy setup
            if self.config.verbose:
                print("Calling strategy.setup()")
            self.strategy.setup(self.context)

            # Run warm-up executions
            if self.config.num_warmup_runs > 0:
                if self.config.verbose:
                    print(f"Running {self.config.num_warmup_runs} warm-up executions...")
                for i in range(self.config.num_warmup_runs):
                    self.context.execution_number = i + 1
                    try:
                        with time_execution() as timing:
                            self.strategy.execute(self.context)
                    except Exception as e:
                        if self.config.verbose:
                            print(f"Warm-up execution {i + 1} failed: {e}")

            # Run actual executions
            if self.config.verbose:
                print(f"Running {self.config.num_executions} experiment executions...")
            for i in range(self.config.num_executions):
                self.context.execution_number = i + 1
                try:
                    with time_execution() as timing:
                        result = self.strategy.execute(self.context)
                        # Override duration if strategy returned one
                        if result.duration_ms == 0.0:
                            result.duration_ms = timing["duration_ms"]
                except Exception as e:
                    if self.config.verbose:
                        print(f"Execution {i + 1} failed: {e}")
                    result = ExperimentResult(
                        duration_ms=0.0,
                        error=str(e)
                    )

                self.collector.add_result(result)

                if self.config.verbose:
                    status = "✓" if not result.error else "✗"
                    print(f"  {status} Execution {i + 1}/{self.config.num_executions}: {result.duration_ms:.3f}ms")

            # Call strategy teardown
            if self.config.verbose:
                print("Calling strategy.teardown()")
            self.strategy.teardown(self.context)

            # Close database connection
            if db_conn:
                db_conn.close()

            # Run teardown steps
            for step in self.config.teardown_steps:
                if self.config.verbose:
                    print(f"Running teardown step: {step.__name__}")
                step()

            # Export results
            csv_path = self.collector.export_to_csv()
            if self.config.verbose:
                print(f"\nResults exported to: {csv_path}")

            # Print summary
            self.collector.print_summary()

        except Exception as e:
            print(f"Experiment failed: {e}")
            raise

        return self.collector

    def _create_database_connection(self) -> duckdb.DuckDBPyConnection:
        """Create database connection from configuration.

        Returns:
            DuckDB connection
        """
        db_config = self.config.database_config

        # Extract connection parameters
        database = db_config.get("database", ":memory:")
        config_dict = db_config.get("config", {})

        # Create connection
        conn = duckdb.connect(database=database, config=config_dict)

        # Load extensions if specified
        extensions = db_config.get("extensions", [])
        for ext in extensions:
            if isinstance(ext, str):
                conn.execute(f"LOAD '{ext}'")
            elif isinstance(ext, dict):
                # Support dict format: {"name": "extension_name", "path": "optional_path"}
                ext_name = ext.get("name")
                ext_path = ext.get("path")
                if ext_path:
                    conn.execute(f"LOAD '{ext_path}'")
                else:
                    conn.execute(f"LOAD '{ext_name}'")

        return conn

    def _apply_db_settings(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Apply DuckDB settings to database connection.

        Args:
            conn: DuckDB connection
        """
        if not self.config.db_settings:
            return

        for key, value in self.config.db_settings.items():
            try:
                if isinstance(value, (int, float, str)):
                    conn.execute(f"SET {key} = {value}")
                elif isinstance(value, bool):
                    conn.execute(f"SET {key} = {'true' if value else 'false'}")
                else:
                    conn.execute(f"SET {key} = '{value}'")
            except Exception as e:
                if self.config.verbose:
                    print(f"Warning: Failed to set {key} = {value}: {e}")
