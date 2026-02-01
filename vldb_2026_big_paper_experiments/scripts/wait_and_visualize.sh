#!/bin/bash
# Wait for experiments to complete and then generate visualizations

cd "$(dirname "$0")/.."

echo "Waiting for experiments to complete..."
while [ ! -f results/microbenchmark_results.csv ]; do
    sleep 5
    if ps aux | grep -q "[r]un_microbenchmarks.py"; then
        echo "Experiments still running..."
    else
        echo "Experiments may have finished, checking for CSV..."
        sleep 2
        break
    fi
done

if [ -f results/microbenchmark_results.csv ]; then
    echo "Experiments complete! Generating visualizations..."
    source .venv/bin/activate
    python src/vldb_experiments/visualizations.py results/microbenchmark_results.csv results
    echo ""
    echo "Visualizations generated in results/ directory:"
    ls -lh results/*_performance.html 2>/dev/null || echo "No visualization files found"
else
    echo "CSV file not found. Experiments may have failed."
    exit 1
fi
