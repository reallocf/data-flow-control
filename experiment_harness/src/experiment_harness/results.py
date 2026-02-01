"""Result collection and CSV export functionality."""

import csv
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
import statistics
from typing import Any, Dict, List, Optional, Set


@dataclass
class ExperimentResult:
    """Result from a single experiment execution.
    
    Attributes:
        duration_ms: Execution duration in milliseconds
        custom_metrics: Dictionary of custom metric names to values
        timestamp: Timestamp when result was collected (auto-set if not provided)
        error: Optional error message if execution failed
    """

    duration_ms: float
    custom_metrics: Dict[str, Any] = field(default_factory=dict)
    timestamp: Optional[datetime] = None
    error: Optional[str] = None

    def __post_init__(self):
        """Set timestamp if not provided."""
        if self.timestamp is None:
            self.timestamp = datetime.now()


class ResultCollector:
    """Collects and aggregates experiment results, exports to CSV."""

    def __init__(self, output_dir: str, output_filename: str):
        """Initialize result collector.
        
        Args:
            output_dir: Directory for CSV output
            output_filename: Base filename for CSV output
        """
        self.output_dir = Path(output_dir)
        self.output_filename = output_filename
        self.results: List[ExperimentResult] = []
        self.metric_names: Set[str] = set()

    def add_result(self, result: ExperimentResult) -> None:
        """Add a result to the collection.
        
        Args:
            result: Experiment result to add
        """
        self.results.append(result)
        self.metric_names.update(result.custom_metrics.keys())

    def get_all_metric_names(self) -> List[str]:
        """Get sorted list of all metric names collected.
        
        Returns:
            Sorted list of metric name strings
        """
        return sorted(self.metric_names)

    def export_to_csv(self) -> str:
        """Export results to CSV file.
        
        Returns:
            Path to the created CSV file
        """
        self.output_dir.mkdir(parents=True, exist_ok=True)
        csv_path = self.output_dir / self.output_filename

        metric_names = self.get_all_metric_names()

        with open(csv_path, "w", newline="") as f:
            fieldnames = ["execution_number", "timestamp", "duration_ms", "error"] + metric_names
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            for i, result in enumerate(self.results, start=1):
                row = {
                    "execution_number": i,
                    "timestamp": result.timestamp.isoformat() if result.timestamp else "",
                    "duration_ms": result.duration_ms,
                    "error": result.error or "",
                }
                for metric_name in metric_names:
                    row[metric_name] = result.custom_metrics.get(metric_name, "")
                writer.writerow(row)

        return str(csv_path)

    def _calculate_summary(self, metric_names: List[str]) -> Dict[str, Any]:
        """Calculate summary statistics for numeric metrics.
        
        Args:
            metric_names: List of metric names to calculate statistics for
            
        Returns:
            Dictionary with summary statistics (mean, median, stddev, min, max)
        """
        summary: Dict[str, Any] = {
            "execution_number": "summary",
            "timestamp": "",
            "duration_ms": "",
            "error": "",
        }

        # Calculate statistics for duration_ms
        durations = [r.duration_ms for r in self.results]
        if durations:
            mean_dur = statistics.mean(durations)
            median_dur = statistics.median(durations)
            stddev_dur = statistics.stdev(durations) if len(durations) > 1 else 0.0
            min_dur = min(durations)
            max_dur = max(durations)
            summary["duration_ms"] = f"mean={mean_dur:.3f},median={median_dur:.3f},stddev={stddev_dur:.3f},min={min_dur:.3f},max={max_dur:.3f}"

        # Calculate statistics for each custom metric
        for metric_name in metric_names:
            values = []
            for result in self.results:
                value = result.custom_metrics.get(metric_name)
                if value is not None and isinstance(value, (int, float)):
                    values.append(value)

            if values:
                mean_val = statistics.mean(values)
                median_val = statistics.median(values)
                stddev_val = statistics.stdev(values) if len(values) > 1 else 0.0
                min_val = min(values)
                max_val = max(values)
                summary[metric_name] = f"mean={mean_val:.3f},median={median_val:.3f},stddev={stddev_val:.3f},min={min_val:.3f},max={max_val:.3f}"
            else:
                summary[metric_name] = ""

        return summary

    def print_summary(self) -> None:
        """Print summary of results to console."""
        if not self.results:
            print("No results collected.")
            return

        successful = [r for r in self.results if not r.error]
        failed = [r for r in self.results if r.error]

        print("\nExperiment Results Summary:")
        print(f"  Total executions: {len(self.results)}")
        print(f"  Successful: {len(successful)}")
        print(f"  Failed: {len(failed)}")

        if successful:
            durations = [r.duration_ms for r in successful]
            print("\n  Duration (ms):")
            print(f"    Mean: {statistics.mean(durations):.3f}")
            print(f"    Median: {statistics.median(durations):.3f}")
            if len(durations) > 1:
                print(f"    Std Dev: {statistics.stdev(durations):.3f}")
            print(f"    Min: {min(durations):.3f}")
            print(f"    Max: {max(durations):.3f}")

        if self.metric_names:
            print("\n  Custom Metrics:")
            for metric_name in self.get_all_metric_names():
                values = []
                for result in successful:
                    value = result.custom_metrics.get(metric_name)
                    if value is not None and isinstance(value, (int, float)):
                        values.append(value)

                if values:
                    print(f"    {metric_name}:")
                    print(f"      Mean: {statistics.mean(values):.3f}")
                    print(f"      Median: {statistics.median(values):.3f}")
                    if len(values) > 1:
                        print(f"      Std Dev: {statistics.stdev(values):.3f}")
                    print(f"      Min: {min(values):.3f}")
                    print(f"      Max: {max(values):.3f}")
