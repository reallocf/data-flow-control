"""Tax-agent strategy comparing No Policy vs 1Phase with OpenAI and Bedrock models."""

from __future__ import annotations

import contextlib
from dataclasses import replace
import json
from pathlib import Path
import time
from typing import Any

from experiment_harness import ExperimentContext, ExperimentResult, ExperimentStrategy
from langchain_core.messages import ToolMessage
from sql_rewriter import DFCPolicy, Resolution

DEFAULT_POLICY_COUNTS = [0, 1, 2, 4, 8, 16, 32]
DEFAULT_RUNS_PER_SETTING = 5
DEFAULT_MAX_ITERATIONS = 40
DEFAULT_CLAUDE_MODEL = "claude-4.6-opus"
DEFAULT_GPT_MODEL = "gpt-5.2"


def _create_tax_tables(db_path: str) -> None:
    import duckdb

    conn = duckdb.connect(db_path)
    try:
        conn.execute("DROP TABLE IF EXISTS receipts")
        conn.execute("DROP TABLE IF EXISTS expenses")

        conn.execute(
            """
            CREATE TABLE receipts AS
            SELECT *
            FROM (VALUES
              (1, DATE '2025-01-01', 'Airline', 2.13, 'USD', 'TRAVEL', 'CARD', 'New York', 'NY', 'US', FALSE, NULL, 'VEND-1', 'Business expense', 3, 2, TRUE, 2025, TIMESTAMP '2025-01-01 08:00:00'),
              (2, DATE '2025-01-02', 'Office Depot', 3.26, 'USD', 'OFFICE', 'CARD', 'San Francisco', 'CA', 'US', TRUE, 'PRJ-2002', 'VEND-2', 'Business expense', 6, 3, TRUE, 2025, TIMESTAMP '2025-01-01 09:00:00'),
              (3, DATE '2025-01-03', 'Gas Station', 4.39, 'USD', 'TRANSPORT', 'CARD', 'New York', 'NY', 'US', FALSE, 'PRJ-1001', 'VEND-3', 'Business expense', 9, 4, TRUE, 2025, TIMESTAMP '2025-01-01 10:00:00'),
              (4, DATE '2025-01-04', 'Hotel', 5.52, 'USD', 'SOFTWARE', 'CARD', 'San Francisco', 'CA', 'US', TRUE, NULL, 'VEND-4', 'Business expense', 12, 5, FALSE, 2025, TIMESTAMP '2025-01-01 11:00:00'),
              (5, DATE '2025-01-05', 'Cloud Vendor', 6.65, 'USD', 'OTHER', 'CASH', 'New York', 'NY', 'US', FALSE, 'PRJ-2002', 'VEND-5', 'Business expense', 15, 6, TRUE, 2025, TIMESTAMP '2025-01-01 12:00:00'),
              (6, DATE '2025-01-06', 'Taxi', 7.78, 'USD', 'MEAL', 'CARD', 'San Francisco', 'CA', 'US', TRUE, 'PRJ-1001', 'VEND-6', 'Business expense', 18, 7, TRUE, 2025, TIMESTAMP '2025-01-01 13:00:00'),
              (7, DATE '2025-01-07', 'Restaurant', 8.91, 'USD', 'TRAVEL', 'CARD', 'New York', 'NY', 'US', FALSE, NULL, 'VEND-7', 'Business expense', 21, 8, TRUE, 2025, TIMESTAMP '2025-01-01 14:00:00'),
              (8, DATE '2025-01-08', 'Airline', 10.04, 'USD', 'OFFICE', 'CARD', 'San Francisco', 'CA', 'US', TRUE, 'PRJ-2002', 'VEND-8', 'Business expense', 24, 1, FALSE, 2025, TIMESTAMP '2025-01-01 15:00:00'),
              (9, DATE '2025-01-09', 'Office Depot', 11.17, 'USD', 'TRANSPORT', 'CARD', 'New York', 'NY', 'US', FALSE, 'PRJ-1001', 'VEND-9', '', 27, 2, TRUE, 2025, TIMESTAMP '2025-01-01 16:00:00'),
              (10, DATE '2025-01-10', 'Gas Station', 12.3, 'USD', 'SOFTWARE', 'CASH', 'San Francisco', 'CA', 'US', TRUE, NULL, NULL, 'Business expense', 30, 3, TRUE, 2025, TIMESTAMP '2025-01-01 17:00:00'),
              (11, DATE '2025-01-11', 'Hotel', 13.43, 'USD', 'OTHER', 'CARD', 'New York', 'NY', 'US', FALSE, 'PRJ-2002', 'VEND-11', 'Business expense', 33, 4, TRUE, 2025, TIMESTAMP '2025-01-01 18:00:00'),
              (12, DATE '2025-01-12', 'Cloud Vendor', 14.56, 'USD', 'MEAL', 'CARD', 'San Francisco', 'CA', 'US', TRUE, 'PRJ-1001', 'VEND-12', 'Business expense', 36, 5, FALSE, 2025, TIMESTAMP '2025-01-01 19:00:00'),
              (13, DATE '2025-01-13', 'Taxi', 15.69, 'USD', 'TRAVEL', 'CARD', 'New York', 'NY', 'US', FALSE, NULL, 'VEND-13', 'Business expense', 39, 6, TRUE, 2025, TIMESTAMP '2025-01-01 20:00:00'),
              (14, DATE '2025-01-14', 'Restaurant', 16.82, 'USD', 'OFFICE', 'CARD', 'San Francisco', 'CA', 'US', TRUE, 'PRJ-2002', 'VEND-14', 'Business expense', 42, 7, TRUE, 2025, TIMESTAMP '2025-01-01 21:00:00'),
              (15, DATE '2025-01-15', 'Airline', 17.95, 'USD', 'TRANSPORT', 'CASH', 'New York', 'NY', 'US', FALSE, 'PRJ-1001', 'VEND-15', 'Business expense', 45, 8, TRUE, 2025, TIMESTAMP '2025-01-01 22:00:00'),
              (16, DATE '2025-01-16', 'Office Depot', 19.08, 'USD', 'SOFTWARE', 'CARD', 'San Francisco', 'CA', 'US', TRUE, NULL, 'VEND-16', 'Business expense', 48, 1, FALSE, 2025, TIMESTAMP '2025-01-01 23:00:00'),
              (17, DATE '2025-01-17', 'Gas Station', 20.21, 'USD', 'OTHER', 'CARD', 'New York', 'NY', 'US', FALSE, 'PRJ-2002', 'VEND-17', 'Business expense', 51, 2, TRUE, 2025, TIMESTAMP '2025-01-02 00:00:00'),
              (18, DATE '2025-01-18', 'Hotel', 21.34, 'USD', 'MEAL', 'CARD', 'San Francisco', 'CA', 'US', TRUE, 'PRJ-1001', 'VEND-18', '', 54, 3, TRUE, 2025, TIMESTAMP '2025-01-02 01:00:00'),
              (19, DATE '2025-01-19', 'Cloud Vendor', 22.47, 'USD', 'TRAVEL', 'CARD', 'New York', 'NY', 'US', FALSE, NULL, 'VEND-19', 'Business expense', 57, 4, TRUE, 2025, TIMESTAMP '2025-01-02 02:00:00'),
              (20, DATE '2025-01-20', 'Taxi', 23.6, 'USD', 'OFFICE', 'CASH', 'San Francisco', 'CA', 'US', TRUE, 'PRJ-2002', NULL, 'Business expense', 60, 5, FALSE, 2025, TIMESTAMP '2025-01-02 03:00:00'),
              (21, DATE '2025-01-21', 'Restaurant', 24.73, 'USD', 'TRANSPORT', 'CARD', 'New York', 'NY', 'US', FALSE, 'PRJ-1001', 'VEND-21', 'Business expense', 63, 6, TRUE, 2025, TIMESTAMP '2025-01-02 04:00:00'),
              (22, DATE '2025-01-22', 'Airline', 25.86, 'USD', 'SOFTWARE', 'CARD', 'San Francisco', 'CA', 'US', TRUE, NULL, 'VEND-22', 'Business expense', 66, 7, TRUE, 2025, TIMESTAMP '2025-01-02 05:00:00'),
              (23, DATE '2025-01-23', 'Office Depot', 26.99, 'USD', 'OTHER', 'CARD', 'New York', 'NY', 'US', FALSE, 'PRJ-2002', 'VEND-23', 'Business expense', 69, 8, TRUE, 2025, TIMESTAMP '2025-01-02 06:00:00'),
              (24, DATE '2025-01-24', 'Gas Station', 28.12, 'USD', 'MEAL', 'CARD', 'San Francisco', 'CA', 'US', TRUE, 'PRJ-1001', 'VEND-24', 'Business expense', 72, 1, FALSE, 2025, TIMESTAMP '2025-01-02 07:00:00'),
              (25, DATE '2025-01-25', 'Hotel', 29.25, 'USD', 'TRAVEL', 'CASH', 'New York', 'NY', 'US', FALSE, NULL, 'VEND-25', 'Business expense', 75, 2, TRUE, 2025, TIMESTAMP '2025-01-02 08:00:00'),
              (26, DATE '2025-01-26', 'Cloud Vendor', 30.38, 'USD', 'OFFICE', 'CARD', 'San Francisco', 'CA', 'US', TRUE, 'PRJ-2002', 'VEND-26', 'Business expense', 78, 3, TRUE, 2025, TIMESTAMP '2025-01-02 09:00:00'),
              (27, DATE '2025-01-27', 'Taxi', 31.51, 'USD', 'TRANSPORT', 'CARD', 'New York', 'NY', 'US', FALSE, 'PRJ-1001', 'VEND-27', '', 81, 4, TRUE, 2025, TIMESTAMP '2025-01-02 10:00:00'),
              (28, DATE '2025-01-28', 'Restaurant', 32.64, 'USD', 'SOFTWARE', 'CARD', 'San Francisco', 'CA', 'US', TRUE, NULL, 'VEND-28', 'Business expense', 84, 5, FALSE, 2025, TIMESTAMP '2025-01-02 11:00:00'),
              (29, DATE '2025-01-29', 'Airline', 33.77, 'USD', 'OTHER', 'CARD', 'New York', 'NY', 'US', FALSE, 'PRJ-2002', 'VEND-29', 'Business expense', 87, 6, TRUE, 2025, TIMESTAMP '2025-01-02 12:00:00'),
              (30, DATE '2025-01-30', 'Office Depot', 34.9, 'USD', 'MEAL', 'CASH', 'San Francisco', 'CA', 'US', TRUE, 'PRJ-1001', NULL, 'Business expense', 90, 7, TRUE, 2025, TIMESTAMP '2025-01-02 13:00:00'),
              (31, DATE '2025-01-31', 'Gas Station', 36.03, 'USD', 'TRAVEL', 'CARD', 'New York', 'NY', 'US', FALSE, NULL, 'VEND-31', 'Business expense', 93, 8, TRUE, 2025, TIMESTAMP '2025-01-02 14:00:00'),
              (32, DATE '2025-02-01', 'Hotel', 37.16, 'USD', 'OFFICE', 'CARD', 'San Francisco', 'CA', 'US', TRUE, 'PRJ-2002', 'VEND-32', 'Business expense', 96, 1, FALSE, 2025, TIMESTAMP '2025-01-02 15:00:00'),
              (33, DATE '2025-02-02', 'Cloud Vendor', 38.29, 'USD', 'TRANSPORT', 'CARD', 'New York', 'NY', 'US', FALSE, 'PRJ-1001', 'VEND-33', 'Business expense', 99, 2, TRUE, 2025, TIMESTAMP '2025-01-02 16:00:00'),
              (34, DATE '2025-02-03', 'Taxi', 39.42, 'USD', 'SOFTWARE', 'CARD', 'San Francisco', 'CA', 'US', TRUE, NULL, 'VEND-34', 'Business expense', 102, 3, TRUE, 2025, TIMESTAMP '2025-01-02 17:00:00'),
              (35, DATE '2025-02-04', 'Restaurant', 40.55, 'USD', 'OTHER', 'CASH', 'New York', 'NY', 'US', FALSE, 'PRJ-2002', 'VEND-35', 'Business expense', 105, 4, TRUE, 2025, TIMESTAMP '2025-01-02 18:00:00'),
              (36, DATE '2025-02-05', 'Airline', 41.68, 'USD', 'MEAL', 'CARD', 'San Francisco', 'CA', 'US', TRUE, 'PRJ-1001', 'VEND-36', '', 108, 5, FALSE, 2025, TIMESTAMP '2025-01-02 19:00:00'),
              (37, DATE '2025-02-06', 'Office Depot', 42.81, 'USD', 'TRAVEL', 'CARD', 'New York', 'NY', 'US', FALSE, NULL, 'VEND-37', 'Business expense', 111, 6, TRUE, 2025, TIMESTAMP '2025-01-02 20:00:00'),
              (38, DATE '2025-02-07', 'Gas Station', 43.94, 'USD', 'OFFICE', 'CARD', 'San Francisco', 'CA', 'US', TRUE, 'PRJ-2002', 'VEND-38', 'Business expense', 114, 7, TRUE, 2025, TIMESTAMP '2025-01-02 21:00:00'),
              (39, DATE '2025-02-08', 'Hotel', 45.07, 'USD', 'TRANSPORT', 'CARD', 'New York', 'NY', 'US', FALSE, 'PRJ-1001', 'VEND-39', 'Business expense', 117, 8, TRUE, 2025, TIMESTAMP '2025-01-02 22:00:00'),
              (40, DATE '2025-02-09', 'Cloud Vendor', 46.2, 'USD', 'SOFTWARE', 'CASH', 'San Francisco', 'CA', 'US', TRUE, NULL, NULL, 'Business expense', 120, 1, FALSE, 2025, TIMESTAMP '2025-01-02 23:00:00'),
              (41, DATE '2025-02-10', 'Taxi', 47.33, 'USD', 'OTHER', 'CARD', 'New York', 'NY', 'US', FALSE, 'PRJ-2002', 'VEND-41', 'Business expense', 123, 2, TRUE, 2025, TIMESTAMP '2025-01-03 00:00:00'),
              (42, DATE '2025-02-11', 'Restaurant', 48.46, 'USD', 'MEAL', 'CARD', 'San Francisco', 'CA', 'US', TRUE, 'PRJ-1001', 'VEND-42', 'Business expense', 126, 3, TRUE, 2025, TIMESTAMP '2025-01-03 01:00:00'),
              (43, DATE '2025-02-12', 'Airline', 49.59, 'USD', 'TRAVEL', 'CARD', 'New York', 'NY', 'US', FALSE, NULL, 'VEND-43', 'Business expense', 129, 4, TRUE, 2025, TIMESTAMP '2025-01-03 02:00:00'),
              (44, DATE '2025-02-13', 'Office Depot', 50.72, 'USD', 'OFFICE', 'CARD', 'San Francisco', 'CA', 'US', TRUE, 'PRJ-2002', 'VEND-44', 'Business expense', 132, 5, FALSE, 2025, TIMESTAMP '2025-01-03 03:00:00'),
              (45, DATE '2025-02-14', 'Gas Station', 51.85, 'USD', 'TRANSPORT', 'CASH', 'New York', 'NY', 'US', FALSE, 'PRJ-1001', 'VEND-45', '', 135, 6, TRUE, 2025, TIMESTAMP '2025-01-03 04:00:00'),
              (46, DATE '2025-02-15', 'Hotel', 52.98, 'USD', 'SOFTWARE', 'CARD', 'San Francisco', 'CA', 'US', TRUE, NULL, 'VEND-46', 'Business expense', 138, 7, TRUE, 2025, TIMESTAMP '2025-01-03 05:00:00'),
              (47, DATE '2025-02-16', 'Cloud Vendor', 54.11, 'USD', 'OTHER', 'CARD', 'New York', 'NY', 'US', FALSE, 'PRJ-2002', 'VEND-47', 'Business expense', 141, 8, TRUE, 2025, TIMESTAMP '2025-01-03 06:00:00'),
              (48, DATE '2025-02-17', 'Taxi', 55.24, 'USD', 'MEAL', 'CARD', 'San Francisco', 'CA', 'US', TRUE, 'PRJ-1001', 'VEND-48', 'Business expense', 144, 1, FALSE, 2025, TIMESTAMP '2025-01-03 07:00:00'),
              (49, DATE '2025-02-18', 'Restaurant', 56.37, 'USD', 'TRAVEL', 'CARD', 'New York', 'NY', 'US', FALSE, NULL, 'VEND-49', 'Business expense', 147, 2, TRUE, 2025, TIMESTAMP '2025-01-03 08:00:00'),
              (50, DATE '2025-02-19', 'Airline', 57.5, 'USD', 'OFFICE', 'CASH', 'San Francisco', 'CA', 'US', TRUE, 'PRJ-2002', NULL, 'Business expense', 150, 3, TRUE, 2025, TIMESTAMP '2025-01-03 09:00:00'),
              (51, DATE '2025-02-20', 'Office Depot', 58.63, 'USD', 'TRANSPORT', 'CARD', 'New York', 'NY', 'US', FALSE, 'PRJ-1001', 'VEND-51', 'Business expense', 153, 4, TRUE, 2025, TIMESTAMP '2025-01-03 10:00:00'),
              (52, DATE '2025-02-21', 'Gas Station', 59.76, 'USD', 'SOFTWARE', 'CARD', 'San Francisco', 'CA', 'US', TRUE, NULL, 'VEND-52', 'Business expense', 156, 5, FALSE, 2025, TIMESTAMP '2025-01-03 11:00:00'),
              (53, DATE '2025-02-22', 'Hotel', 60.89, 'USD', 'OTHER', 'CARD', 'New York', 'NY', 'US', FALSE, 'PRJ-2002', 'VEND-53', 'Business expense', 159, 6, TRUE, 2025, TIMESTAMP '2025-01-03 12:00:00'),
              (54, DATE '2025-02-23', 'Cloud Vendor', 62.02, 'USD', 'MEAL', 'CARD', 'San Francisco', 'CA', 'US', TRUE, 'PRJ-1001', 'VEND-54', '', 162, 7, TRUE, 2025, TIMESTAMP '2025-01-03 13:00:00'),
              (55, DATE '2025-02-24', 'Taxi', 63.15, 'USD', 'TRAVEL', 'CASH', 'New York', 'NY', 'US', FALSE, NULL, 'VEND-55', 'Business expense', 165, 8, TRUE, 2025, TIMESTAMP '2025-01-03 14:00:00'),
              (56, DATE '2025-02-25', 'Restaurant', 64.28, 'USD', 'OFFICE', 'CARD', 'San Francisco', 'CA', 'US', TRUE, 'PRJ-2002', 'VEND-56', 'Business expense', 168, 1, FALSE, 2025, TIMESTAMP '2025-01-03 15:00:00'),
              (57, DATE '2025-02-26', 'Airline', 65.41, 'USD', 'TRANSPORT', 'CARD', 'New York', 'NY', 'US', FALSE, 'PRJ-1001', 'VEND-57', 'Business expense', 171, 2, TRUE, 2025, TIMESTAMP '2025-01-03 16:00:00'),
              (58, DATE '2025-02-27', 'Office Depot', 66.54, 'USD', 'SOFTWARE', 'CARD', 'San Francisco', 'CA', 'US', TRUE, NULL, 'VEND-58', 'Business expense', 174, 3, TRUE, 2025, TIMESTAMP '2025-01-03 17:00:00'),
              (59, DATE '2025-02-28', 'Gas Station', 67.67, 'USD', 'OTHER', 'CARD', 'New York', 'NY', 'US', FALSE, 'PRJ-2002', 'VEND-59', 'Business expense', 177, 4, TRUE, 2025, TIMESTAMP '2025-01-03 18:00:00'),
              (60, DATE '2025-03-01', 'Hotel', 68.8, 'USD', 'MEAL', 'CASH', 'San Francisco', 'CA', 'US', TRUE, 'PRJ-1001', NULL, 'Business expense', 180, 5, FALSE, 2025, TIMESTAMP '2025-01-03 19:00:00'),
              (61, DATE '2025-03-02', 'Cloud Vendor', 69.93, 'USD', 'TRAVEL', 'CARD', 'New York', 'NY', 'US', FALSE, NULL, 'VEND-61', 'Business expense', 183, 6, TRUE, 2025, TIMESTAMP '2025-01-03 20:00:00'),
              (62, DATE '2025-03-03', 'Taxi', 71.06, 'USD', 'OFFICE', 'CARD', 'San Francisco', 'CA', 'US', TRUE, 'PRJ-2002', 'VEND-62', 'Business expense', 186, 7, TRUE, 2025, TIMESTAMP '2025-01-03 21:00:00'),
              (63, DATE '2025-03-04', 'Restaurant', 72.19, 'USD', 'TRANSPORT', 'CARD', 'New York', 'NY', 'US', FALSE, 'PRJ-1001', 'VEND-63', '', 189, 8, TRUE, 2025, TIMESTAMP '2025-01-03 22:00:00'),
              (64, DATE '2025-03-05', 'Airline', 73.32, 'USD', 'SOFTWARE', 'CARD', 'San Francisco', 'CA', 'US', TRUE, NULL, 'VEND-64', 'Business expense', 192, 1, FALSE, 2025, TIMESTAMP '2025-01-03 23:00:00'),
              (65, DATE '2025-03-06', 'Office Depot', 74.45, 'USD', 'OTHER', 'CASH', 'New York', 'NY', 'US', FALSE, 'PRJ-2002', 'VEND-65', 'Business expense', 195, 2, TRUE, 2025, TIMESTAMP '2025-01-04 00:00:00'),
              (66, DATE '2025-03-07', 'Gas Station', 75.58, 'USD', 'MEAL', 'CARD', 'San Francisco', 'CA', 'US', TRUE, 'PRJ-1001', 'VEND-66', 'Business expense', 198, 3, TRUE, 2025, TIMESTAMP '2025-01-04 01:00:00'),
              (67, DATE '2025-03-08', 'Hotel', 76.71, 'USD', 'TRAVEL', 'CARD', 'New York', 'NY', 'US', FALSE, NULL, 'VEND-67', 'Business expense', 201, 4, TRUE, 2025, TIMESTAMP '2025-01-04 02:00:00'),
              (68, DATE '2025-03-09', 'Cloud Vendor', 77.84, 'USD', 'OFFICE', 'CARD', 'San Francisco', 'CA', 'US', TRUE, 'PRJ-2002', 'VEND-68', 'Business expense', 204, 5, FALSE, 2025, TIMESTAMP '2025-01-04 03:00:00'),
              (69, DATE '2025-03-10', 'Taxi', 78.97, 'USD', 'TRANSPORT', 'CARD', 'New York', 'NY', 'US', FALSE, 'PRJ-1001', 'VEND-69', 'Business expense', 207, 6, TRUE, 2025, TIMESTAMP '2025-01-04 04:00:00'),
              (70, DATE '2025-03-11', 'Restaurant', 80.1, 'USD', 'SOFTWARE', 'CASH', 'San Francisco', 'CA', 'US', TRUE, NULL, NULL, 'Business expense', 210, 7, TRUE, 2025, TIMESTAMP '2025-01-04 05:00:00'),
              (71, DATE '2025-03-12', 'Airline', 81.23, 'USD', 'OTHER', 'CARD', 'New York', 'NY', 'US', FALSE, 'PRJ-2002', 'VEND-71', 'Business expense', 213, 8, TRUE, 2025, TIMESTAMP '2025-01-04 06:00:00'),
              (72, DATE '2025-03-13', 'Office Depot', 82.36, 'USD', 'MEAL', 'CARD', 'San Francisco', 'CA', 'US', TRUE, 'PRJ-1001', 'VEND-72', '', 216, 1, FALSE, 2025, TIMESTAMP '2025-01-04 07:00:00'),
              (73, DATE '2025-03-14', 'Gas Station', 83.49, 'USD', 'TRAVEL', 'CARD', 'New York', 'NY', 'US', FALSE, NULL, 'VEND-73', 'Business expense', 219, 2, TRUE, 2025, TIMESTAMP '2025-01-04 08:00:00'),
              (74, DATE '2025-03-15', 'Hotel', 84.62, 'USD', 'OFFICE', 'CARD', 'San Francisco', 'CA', 'US', TRUE, 'PRJ-2002', 'VEND-74', 'Business expense', 222, 3, TRUE, 2025, TIMESTAMP '2025-01-04 09:00:00'),
              (75, DATE '2025-03-16', 'Cloud Vendor', 85.75, 'USD', 'TRANSPORT', 'CASH', 'New York', 'NY', 'US', FALSE, 'PRJ-1001', 'VEND-75', 'Business expense', 225, 4, TRUE, 2025, TIMESTAMP '2025-01-04 10:00:00'),
              (76, DATE '2025-03-17', 'Taxi', 86.88, 'USD', 'SOFTWARE', 'CARD', 'San Francisco', 'CA', 'US', TRUE, NULL, 'VEND-76', 'Business expense', 228, 5, FALSE, 2025, TIMESTAMP '2025-01-04 11:00:00'),
              (77, DATE '2025-03-18', 'Restaurant', 88.01, 'USD', 'OTHER', 'CARD', 'New York', 'NY', 'US', FALSE, 'PRJ-2002', 'VEND-77', 'Business expense', 231, 6, TRUE, 2025, TIMESTAMP '2025-01-04 12:00:00'),
              (78, DATE '2025-03-19', 'Airline', 89.14, 'USD', 'MEAL', 'CARD', 'San Francisco', 'CA', 'US', TRUE, 'PRJ-1001', 'VEND-78', 'Business expense', 234, 7, TRUE, 2025, TIMESTAMP '2025-01-04 13:00:00'),
              (79, DATE '2025-03-20', 'Office Depot', 90.27, 'USD', 'TRAVEL', 'CARD', 'New York', 'NY', 'US', FALSE, NULL, 'VEND-79', 'Business expense', 237, 8, TRUE, 2025, TIMESTAMP '2025-01-04 14:00:00'),
              (80, DATE '2025-03-21', 'Gas Station', 91.4, 'USD', 'OFFICE', 'CASH', 'San Francisco', 'CA', 'US', TRUE, 'PRJ-2002', NULL, 'Business expense', 240, 1, FALSE, 2025, TIMESTAMP '2025-01-04 15:00:00'),
              (81, DATE '2025-03-22', 'Hotel', 92.53, 'USD', 'TRANSPORT', 'CARD', 'New York', 'NY', 'US', FALSE, 'PRJ-1001', 'VEND-81', '', 243, 2, TRUE, 2025, TIMESTAMP '2025-01-04 16:00:00'),
              (82, DATE '2025-03-23', 'Cloud Vendor', 93.66, 'USD', 'SOFTWARE', 'CARD', 'San Francisco', 'CA', 'US', TRUE, NULL, 'VEND-82', 'Business expense', 246, 3, TRUE, 2025, TIMESTAMP '2025-01-04 17:00:00'),
              (83, DATE '2025-03-24', 'Taxi', 94.79, 'USD', 'OTHER', 'CARD', 'New York', 'NY', 'US', FALSE, 'PRJ-2002', 'VEND-83', 'Business expense', 249, 4, TRUE, 2025, TIMESTAMP '2025-01-04 18:00:00'),
              (84, DATE '2025-03-25', 'Restaurant', 95.92, 'USD', 'MEAL', 'CARD', 'San Francisco', 'CA', 'US', TRUE, 'PRJ-1001', 'VEND-84', 'Business expense', 252, 5, FALSE, 2025, TIMESTAMP '2025-01-04 19:00:00'),
              (85, DATE '2025-03-26', 'Airline', 97.05, 'USD', 'TRAVEL', 'CASH', 'New York', 'NY', 'US', FALSE, NULL, 'VEND-85', 'Business expense', 255, 6, TRUE, 2025, TIMESTAMP '2025-01-04 20:00:00'),
              (86, DATE '2025-03-27', 'Office Depot', 98.18, 'USD', 'OFFICE', 'CARD', 'San Francisco', 'CA', 'US', TRUE, 'PRJ-2002', 'VEND-86', 'Business expense', 258, 7, TRUE, 2025, TIMESTAMP '2025-01-04 21:00:00'),
              (87, DATE '2025-03-28', 'Gas Station', 99.31, 'USD', 'TRANSPORT', 'CARD', 'New York', 'NY', 'US', FALSE, 'PRJ-1001', 'VEND-87', 'Business expense', 261, 8, TRUE, 2025, TIMESTAMP '2025-01-04 22:00:00'),
              (88, DATE '2025-03-29', 'Hotel', 100.44, 'USD', 'SOFTWARE', 'CARD', 'San Francisco', 'CA', 'US', TRUE, NULL, 'VEND-88', 'Business expense', 264, 1, FALSE, 2025, TIMESTAMP '2025-01-04 23:00:00'),
              (89, DATE '2025-03-30', 'Cloud Vendor', 101.57, 'USD', 'OTHER', 'CARD', 'New York', 'NY', 'US', FALSE, 'PRJ-2002', 'VEND-89', 'Business expense', 267, 2, TRUE, 2025, TIMESTAMP '2025-01-05 00:00:00'),
              (90, DATE '2025-03-31', 'Taxi', 102.7, 'USD', 'MEAL', 'CASH', 'San Francisco', 'CA', 'US', TRUE, 'PRJ-1001', NULL, '', 270, 3, TRUE, 2025, TIMESTAMP '2025-01-05 01:00:00'),
              (91, DATE '2025-04-01', 'Restaurant', 103.83, 'USD', 'TRAVEL', 'CARD', 'New York', 'NY', 'US', FALSE, NULL, 'VEND-91', 'Business expense', 273, 4, TRUE, 2025, TIMESTAMP '2025-01-05 02:00:00'),
              (92, DATE '2025-04-02', 'Airline', 104.96, 'USD', 'OFFICE', 'CARD', 'San Francisco', 'CA', 'US', TRUE, 'PRJ-2002', 'VEND-92', 'Business expense', 276, 5, FALSE, 2025, TIMESTAMP '2025-01-05 03:00:00'),
              (93, DATE '2025-04-03', 'Office Depot', 106.09, 'USD', 'TRANSPORT', 'CARD', 'New York', 'NY', 'US', FALSE, 'PRJ-1001', 'VEND-93', 'Business expense', 279, 6, TRUE, 2025, TIMESTAMP '2025-01-05 04:00:00'),
              (94, DATE '2025-04-04', 'Gas Station', 107.22, 'USD', 'SOFTWARE', 'CARD', 'San Francisco', 'CA', 'US', TRUE, NULL, 'VEND-94', 'Business expense', 282, 7, TRUE, 2025, TIMESTAMP '2025-01-05 05:00:00'),
              (95, DATE '2025-04-05', 'Hotel', 108.35, 'USD', 'OTHER', 'CASH', 'New York', 'NY', 'US', FALSE, 'PRJ-2002', 'VEND-95', 'Business expense', 285, 8, TRUE, 2025, TIMESTAMP '2025-01-05 06:00:00'),
              (96, DATE '2025-04-06', 'Cloud Vendor', 109.48, 'USD', 'MEAL', 'CARD', 'San Francisco', 'CA', 'US', TRUE, 'PRJ-1001', 'VEND-96', 'Business expense', 288, 1, FALSE, 2025, TIMESTAMP '2025-01-05 07:00:00'),
              (97, DATE '2025-04-07', 'Taxi', 110.61, 'USD', 'TRAVEL', 'CARD', 'New York', 'NY', 'US', FALSE, NULL, 'VEND-97', 'Business expense', 291, 2, TRUE, 2025, TIMESTAMP '2025-01-05 08:00:00'),
              (98, DATE '2025-04-08', 'Restaurant', 111.74, 'USD', 'OFFICE', 'CARD', 'San Francisco', 'CA', 'US', TRUE, 'PRJ-2002', 'VEND-98', 'Business expense', 294, 3, TRUE, 2025, TIMESTAMP '2025-01-05 09:00:00'),
              (99, DATE '2025-04-09', 'Airline', 112.87, 'USD', 'TRANSPORT', 'CARD', 'New York', 'NY', 'US', FALSE, 'PRJ-1001', 'VEND-99', '', 297, 4, TRUE, 2025, TIMESTAMP '2025-01-05 10:00:00'),
              (100, DATE '2025-04-10', 'Office Depot', 114.0, 'USD', 'SOFTWARE', 'CASH', 'San Francisco', 'CA', 'US', TRUE, NULL, NULL, 'Business expense', 300, 5, FALSE, 2025, TIMESTAMP '2025-01-05 11:00:00')
            ) AS receipts(
              receipt_id, tx_date, merchant, amount, currency, category, payment_method, city, state, country,
              client_billable, project_code, vendor_tax_id, business_purpose, miles, attendee_count, has_receipt_image, tax_year, created_at
            );
            """
        )

        conn.execute(
            """
            CREATE TABLE expenses (
              expense_id BIGINT,
              receipt_id BIGINT,
              expense_date DATE,
              vendor_name VARCHAR,
              net_amount DOUBLE,
              tax_amount DOUBLE,
              gross_amount DOUBLE,
              currency_code VARCHAR,
              expense_category VARCHAR,
              deductible_pct INTEGER,
              payment_channel VARCHAR,
              jurisdiction VARCHAR,
              billable_flag BOOLEAN,
              project_tag VARCHAR,
              documentation_quality VARCHAR,
              compliance_notes VARCHAR,
              cannot_categorize_safely BOOLEAN,
              invalid_string VARCHAR
            )
            """
        )
    finally:
        conn.close()


def _policy_catalog() -> list[tuple[str, str]]:
    return [
        (
            "count(distinct receipts.receipt_id) = 1",
            "Each expense row must be derived from exactly one receipt.",
        ),
        (
            "expenses.receipt_id = min(receipts.receipt_id)",
            "Expense receipt_id must match the source receipt_id.",
        ),
        (
            "expenses.expense_id = min(receipts.receipt_id)",
            "expense_id must be deterministically derived as receipt_id.",
        ),
        (
            "expenses.expense_date = min(receipts.tx_date)",
            "expense_date must match receipt tx_date.",
        ),
        (
            "lower(expenses.vendor_name) = lower(min(receipts.merchant))",
            "vendor_name should map to merchant.",
        ),
        (
            "abs(expenses.net_amount - min(receipts.amount)) < 0.01",
            "net_amount must be grounded to receipt amount.",
        ),
        (
            "expenses.gross_amount >= expenses.net_amount",
            "gross_amount must be at least net_amount.",
        ),
        (
            "abs(expenses.gross_amount - (expenses.net_amount + expenses.tax_amount)) < 0.01",
            "gross_amount should equal net_amount + tax_amount.",
        ),
        (
            "expenses.currency_code = min(receipts.currency)",
            "currency_code must match source currency.",
        ),
        (
            "expenses.payment_channel = min(receipts.payment_method)",
            "payment_channel should map from payment_method.",
        ),
        (
            "expenses.billable_flag = min(receipts.client_billable)",
            "billable_flag should map from client_billable.",
        ),
        (
            "expenses.jurisdiction = concat(min(receipts.state), '-', min(receipts.country))",
            "jurisdiction must be normalized from source state/country.",
        ),
        (
            "expenses.project_tag IS NOT DISTINCT FROM min(receipts.project_code)",
            "project_tag should map directly from project_code.",
        ),
        (
            "NOT expenses.billable_flag OR expenses.project_tag IS NOT NULL",
            "Billable expenses require project_tag.",
        ),
        (
            "expenses.documentation_quality IN ('HIGH', 'MEDIUM', 'LOW')",
            "documentation_quality must be HIGH, MEDIUM, or LOW.",
        ),
        (
            "NOT min(receipts.has_receipt_image) OR expenses.documentation_quality IN ('HIGH', 'MEDIUM')",
            "If receipt image exists, documentation_quality cannot be LOW.",
        ),
        (
            "min(receipts.has_receipt_image) OR expenses.documentation_quality = 'LOW'",
            "Missing receipt image should be marked LOW documentation quality.",
        ),
        (
            "expenses.expense_category IN ('MEAL', 'TRAVEL', 'OFFICE', 'TRANSPORT', 'SOFTWARE', 'OTHER')",
            "expense_category must be one of the approved categories.",
        ),
        (
            "expenses.expense_category = min(receipts.category)",
            "expense_category should map from category.",
        ),
        (
            "NOT (expenses.expense_category = 'MEAL') OR expenses.deductible_pct <= 50",
            "Meals must have deductible_pct at most 50.",
        ),
        (
            "NOT (expenses.expense_category = 'SOFTWARE') OR expenses.deductible_pct = 100",
            "Software expenses must be fully deductible.",
        ),
        (
            "NOT (expenses.expense_category = 'OFFICE') OR expenses.deductible_pct = 100",
            "Office expenses should be fully deductible.",
        ),
        (
            "NOT (expenses.expense_category = 'TRAVEL') OR expenses.deductible_pct IN (80, 100)",
            "Travel deductible_pct should be 80 or 100.",
        ),
        (
            "NOT (expenses.expense_category = 'MEAL') OR length(expenses.compliance_notes) > 0",
            "Meals must include compliance_notes.",
        ),
        (
            "NOT (expenses.expense_category = 'TRAVEL') OR length(expenses.compliance_notes) > 0",
            "Travel must include compliance_notes.",
        ),
        (
            "NOT (expenses.expense_category = 'TRANSPORT') OR expenses.payment_channel <> 'CASH'",
            "Transport expenses should not use cash payment_channel.",
        ),
        (
            "NOT (expenses.expense_category = 'OTHER') OR expenses.net_amount <= 1500",
            "Other expenses must have net_amount at most 1500.",
        ),
        (
            "NOT (expenses.expense_category = 'TRAVEL') OR expenses.net_amount <= 4000",
            "Travel must have net_amount at most 4000.",
        ),
        (
            "NOT (expenses.expense_category = 'SOFTWARE') OR expenses.net_amount <= 5000",
            "Software must have net_amount at most 5000.",
        ),
        (
            "NOT expenses.billable_flag OR expenses.compliance_notes LIKE 'Client:%'",
            "Billable expenses must prefix compliance_notes with 'Client:'.",
        ),
        (
            "NOT (expenses.expense_category = 'TRAVEL') OR lower(expenses.vendor_name) IN ('airline', 'hotel', 'taxi')",
            "Travel categories must map to travel-related merchants.",
        ),
        (
            "NOT (expenses.expense_category = 'TRANSPORT') OR lower(expenses.vendor_name) IN ('gas station', 'taxi')",
            "Transport category must map to transport merchants.",
        ),
    ]


def _build_policies(policy_count: int) -> list[DFCPolicy]:
    selected = _policy_catalog()[:policy_count]
    return [
        DFCPolicy(
            sources=["receipts"],
            sink="expenses",
            constraint=f"expenses.cannot_categorize_safely OR ({constraint})",
            on_fail=Resolution.INVALIDATE_MESSAGE,
            description=description,
        )
        for constraint, description in selected
    ]


def _tax_agent_prompt(policy_descriptions: list[str]) -> str:
    policy_block = (
        "\n".join([f"{idx + 1}. {description}" for idx, description in enumerate(policy_descriptions)])
        if policy_descriptions
        else "No active policies for this run."
    )
    return (
        "You are categorizing receipts into expenses for tax filing. "
        "Use execute_sql only.\n\n"
        "Task:\n"
        "1. Insert all 100 rows from receipts into expenses.\n"
        "2. Transform receipt fields into the normalized expenses schema.\n"
        "3. Derive tax-oriented fields (category, deductible %, quality, jurisdiction).\n"
        "4. Use compliance_notes to explain assumptions when needed.\n"
        "5. If a row cannot be safely categorized under active constraints, still insert it but set "
        "cannot_categorize_safely = TRUE.\n"
        "6. After insertion, run a summary query over expenses.\n\n"
        "Active policy descriptions:\n"
        f"{policy_block}\n\n"
        "Required output schema columns in insert order:\n"
        "expense_id, receipt_id, expense_date, vendor_name, net_amount, tax_amount, gross_amount, "
        "currency_code, expense_category, deductible_pct, payment_channel, jurisdiction, billable_flag, "
        "project_tag, documentation_quality, compliance_notes, cannot_categorize_safely"
    )


def _coerce_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict) and item.get("text"):
                parts.append(str(item["text"]))
        return "\n".join(parts).strip()
    return str(content)


def _extract_tool_payloads(chat_history: list[Any]) -> list[dict[str, Any]]:
    payloads: list[dict[str, Any]] = []
    for message in chat_history:
        if not isinstance(message, ToolMessage):
            continue
        raw = _coerce_text(message.content)
        if not raw:
            continue
        try:
            decoded = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(decoded, dict):
            payloads.append(decoded)
    return payloads


def _is_expenses_insert_payload(payload: dict[str, Any]) -> bool:
    rewritten_sql = str(payload.get("rewritten_sql", ""))
    return "insert into expenses" in rewritten_sql.lower()


def _policy_failure_counts(
    tool_payloads: list[dict[str, Any]],
    policy_descriptions: list[str],
) -> dict[str, int]:
    counts: dict[str, int] = dict.fromkeys(policy_descriptions, 0)
    relevant_payloads = [payload for payload in tool_payloads if _is_expenses_insert_payload(payload)]
    if not relevant_payloads:
        relevant_payloads = tool_payloads

    for payload in relevant_payloads:
        violations = payload.get("policy_violations", [])
        if not isinstance(violations, list):
            continue
        for violation in violations:
            if not isinstance(violation, dict):
                continue
            raw_messages = violation.get("policy_messages", [])
            if isinstance(raw_messages, str):
                policy_messages = [raw_messages]
            elif isinstance(raw_messages, list):
                policy_messages = [str(m) for m in raw_messages if str(m).strip()]
            else:
                policy_messages = []
            for policy_message in policy_messages:
                if policy_message in counts:
                    counts[policy_message] += 1
    return counts



EXPECTED_VALUES_SQL_BY_POLICY_COUNT = {
    0: """SELECT *
FROM (VALUES
  (1, 1, DATE '2025-01-01', 'Airline', 2.13, 0.0, 2.13, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (2, 2, DATE '2025-01-02', 'Office Depot', 3.26, 0.0, 3.26, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (3, 3, DATE '2025-01-03', 'Gas Station', 4.39, 0.0, 4.39, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (4, 4, DATE '2025-01-04', 'Hotel', 5.52, 0.0, 5.52, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', FALSE),
  (5, 5, DATE '2025-01-05', 'Cloud Vendor', 6.65, 0.0, 6.65, 'USD', 'OTHER', 100, 'CASH', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (6, 6, DATE '2025-01-06', 'Taxi', 7.78, 0.0, 7.78, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'HIGH', 'Client: auto', FALSE),
  (7, 7, DATE '2025-01-07', 'Restaurant', 8.91, 0.0, 8.91, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (8, 8, DATE '2025-01-08', 'Airline', 10.04, 0.0, 10.04, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'LOW', 'Client: auto', FALSE),
  (9, 9, DATE '2025-01-09', 'Office Depot', 11.17, 0.0, 11.17, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (10, 10, DATE '2025-01-10', 'Gas Station', 12.3, 0.0, 12.3, 'USD', 'SOFTWARE', 100, 'CASH', 'CA-US', TRUE, NULL, 'HIGH', 'Client: auto', FALSE),
  (11, 11, DATE '2025-01-11', 'Hotel', 13.43, 0.0, 13.43, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (12, 12, DATE '2025-01-12', 'Cloud Vendor', 14.56, 0.0, 14.56, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'LOW', 'Client: auto', FALSE),
  (13, 13, DATE '2025-01-13', 'Taxi', 15.69, 0.0, 15.69, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (14, 14, DATE '2025-01-14', 'Restaurant', 16.82, 0.0, 16.82, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (15, 15, DATE '2025-01-15', 'Airline', 17.95, 0.0, 17.95, 'USD', 'TRANSPORT', 100, 'CASH', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (16, 16, DATE '2025-01-16', 'Office Depot', 19.08, 0.0, 19.08, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', FALSE),
  (17, 17, DATE '2025-01-17', 'Gas Station', 20.21, 0.0, 20.21, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (18, 18, DATE '2025-01-18', 'Hotel', 21.34, 0.0, 21.34, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'HIGH', 'Client: auto', FALSE),
  (19, 19, DATE '2025-01-19', 'Cloud Vendor', 22.47, 0.0, 22.47, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (20, 20, DATE '2025-01-20', 'Taxi', 23.6, 0.0, 23.6, 'USD', 'OFFICE', 100, 'CASH', 'CA-US', TRUE, 'PRJ-2002', 'LOW', 'Client: auto', FALSE),
  (21, 21, DATE '2025-01-21', 'Restaurant', 24.73, 0.0, 24.73, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (22, 22, DATE '2025-01-22', 'Airline', 25.86, 0.0, 25.86, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'HIGH', 'Client: auto', FALSE),
  (23, 23, DATE '2025-01-23', 'Office Depot', 26.99, 0.0, 26.99, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (24, 24, DATE '2025-01-24', 'Gas Station', 28.12, 0.0, 28.12, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'LOW', 'Client: auto', FALSE),
  (25, 25, DATE '2025-01-25', 'Hotel', 29.25, 0.0, 29.25, 'USD', 'TRAVEL', 80, 'CASH', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (26, 26, DATE '2025-01-26', 'Cloud Vendor', 30.38, 0.0, 30.38, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (27, 27, DATE '2025-01-27', 'Taxi', 31.51, 0.0, 31.51, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (28, 28, DATE '2025-01-28', 'Restaurant', 32.64, 0.0, 32.64, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', FALSE),
  (29, 29, DATE '2025-01-29', 'Airline', 33.77, 0.0, 33.77, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (30, 30, DATE '2025-01-30', 'Office Depot', 34.9, 0.0, 34.9, 'USD', 'MEAL', 50, 'CASH', 'CA-US', TRUE, 'PRJ-1001', 'HIGH', 'Client: auto', FALSE),
  (31, 31, DATE '2025-01-31', 'Gas Station', 36.03, 0.0, 36.03, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (32, 32, DATE '2025-02-01', 'Hotel', 37.16, 0.0, 37.16, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'LOW', 'Client: auto', FALSE),
  (33, 33, DATE '2025-02-02', 'Cloud Vendor', 38.29, 0.0, 38.29, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (34, 34, DATE '2025-02-03', 'Taxi', 39.42, 0.0, 39.42, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'HIGH', 'Client: auto', FALSE),
  (35, 35, DATE '2025-02-04', 'Restaurant', 40.55, 0.0, 40.55, 'USD', 'OTHER', 100, 'CASH', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (36, 36, DATE '2025-02-05', 'Airline', 41.68, 0.0, 41.68, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'LOW', 'Client: auto', FALSE),
  (37, 37, DATE '2025-02-06', 'Office Depot', 42.81, 0.0, 42.81, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (38, 38, DATE '2025-02-07', 'Gas Station', 43.94, 0.0, 43.94, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (39, 39, DATE '2025-02-08', 'Hotel', 45.07, 0.0, 45.07, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (40, 40, DATE '2025-02-09', 'Cloud Vendor', 46.2, 0.0, 46.2, 'USD', 'SOFTWARE', 100, 'CASH', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', FALSE),
  (41, 41, DATE '2025-02-10', 'Taxi', 47.33, 0.0, 47.33, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (42, 42, DATE '2025-02-11', 'Restaurant', 48.46, 0.0, 48.46, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'HIGH', 'Client: auto', FALSE),
  (43, 43, DATE '2025-02-12', 'Airline', 49.59, 0.0, 49.59, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (44, 44, DATE '2025-02-13', 'Office Depot', 50.72, 0.0, 50.72, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'LOW', 'Client: auto', FALSE),
  (45, 45, DATE '2025-02-14', 'Gas Station', 51.85, 0.0, 51.85, 'USD', 'TRANSPORT', 100, 'CASH', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (46, 46, DATE '2025-02-15', 'Hotel', 52.98, 0.0, 52.98, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'HIGH', 'Client: auto', FALSE),
  (47, 47, DATE '2025-02-16', 'Cloud Vendor', 54.11, 0.0, 54.11, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (48, 48, DATE '2025-02-17', 'Taxi', 55.24, 0.0, 55.24, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'LOW', 'Client: auto', FALSE),
  (49, 49, DATE '2025-02-18', 'Restaurant', 56.37, 0.0, 56.37, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (50, 50, DATE '2025-02-19', 'Airline', 57.5, 0.0, 57.5, 'USD', 'OFFICE', 100, 'CASH', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (51, 51, DATE '2025-02-20', 'Office Depot', 58.63, 0.0, 58.63, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (52, 52, DATE '2025-02-21', 'Gas Station', 59.76, 0.0, 59.76, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', FALSE),
  (53, 53, DATE '2025-02-22', 'Hotel', 60.89, 0.0, 60.89, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (54, 54, DATE '2025-02-23', 'Cloud Vendor', 62.02, 0.0, 62.02, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'HIGH', 'Client: auto', FALSE),
  (55, 55, DATE '2025-02-24', 'Taxi', 63.15, 0.0, 63.15, 'USD', 'TRAVEL', 80, 'CASH', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (56, 56, DATE '2025-02-25', 'Restaurant', 64.28, 0.0, 64.28, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'LOW', 'Client: auto', FALSE),
  (57, 57, DATE '2025-02-26', 'Airline', 65.41, 0.0, 65.41, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (58, 58, DATE '2025-02-27', 'Office Depot', 66.54, 0.0, 66.54, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'HIGH', 'Client: auto', FALSE),
  (59, 59, DATE '2025-02-28', 'Gas Station', 67.67, 0.0, 67.67, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (60, 60, DATE '2025-03-01', 'Hotel', 68.8, 0.0, 68.8, 'USD', 'MEAL', 50, 'CASH', 'CA-US', TRUE, 'PRJ-1001', 'LOW', 'Client: auto', FALSE),
  (61, 61, DATE '2025-03-02', 'Cloud Vendor', 69.93, 0.0, 69.93, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (62, 62, DATE '2025-03-03', 'Taxi', 71.06, 0.0, 71.06, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (63, 63, DATE '2025-03-04', 'Restaurant', 72.19, 0.0, 72.19, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (64, 64, DATE '2025-03-05', 'Airline', 73.32, 0.0, 73.32, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', FALSE),
  (65, 65, DATE '2025-03-06', 'Office Depot', 74.45, 0.0, 74.45, 'USD', 'OTHER', 100, 'CASH', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (66, 66, DATE '2025-03-07', 'Gas Station', 75.58, 0.0, 75.58, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'HIGH', 'Client: auto', FALSE),
  (67, 67, DATE '2025-03-08', 'Hotel', 76.71, 0.0, 76.71, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (68, 68, DATE '2025-03-09', 'Cloud Vendor', 77.84, 0.0, 77.84, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'LOW', 'Client: auto', FALSE),
  (69, 69, DATE '2025-03-10', 'Taxi', 78.97, 0.0, 78.97, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (70, 70, DATE '2025-03-11', 'Restaurant', 80.1, 0.0, 80.1, 'USD', 'SOFTWARE', 100, 'CASH', 'CA-US', TRUE, NULL, 'HIGH', 'Client: auto', FALSE),
  (71, 71, DATE '2025-03-12', 'Airline', 81.23, 0.0, 81.23, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (72, 72, DATE '2025-03-13', 'Office Depot', 82.36, 0.0, 82.36, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'LOW', 'Client: auto', FALSE),
  (73, 73, DATE '2025-03-14', 'Gas Station', 83.49, 0.0, 83.49, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (74, 74, DATE '2025-03-15', 'Hotel', 84.62, 0.0, 84.62, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (75, 75, DATE '2025-03-16', 'Cloud Vendor', 85.75, 0.0, 85.75, 'USD', 'TRANSPORT', 100, 'CASH', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (76, 76, DATE '2025-03-17', 'Taxi', 86.88, 0.0, 86.88, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', FALSE),
  (77, 77, DATE '2025-03-18', 'Restaurant', 88.01, 0.0, 88.01, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (78, 78, DATE '2025-03-19', 'Airline', 89.14, 0.0, 89.14, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'HIGH', 'Client: auto', FALSE),
  (79, 79, DATE '2025-03-20', 'Office Depot', 90.27, 0.0, 90.27, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (80, 80, DATE '2025-03-21', 'Gas Station', 91.4, 0.0, 91.4, 'USD', 'OFFICE', 100, 'CASH', 'CA-US', TRUE, 'PRJ-2002', 'LOW', 'Client: auto', FALSE),
  (81, 81, DATE '2025-03-22', 'Hotel', 92.53, 0.0, 92.53, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (82, 82, DATE '2025-03-23', 'Cloud Vendor', 93.66, 0.0, 93.66, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'HIGH', 'Client: auto', FALSE),
  (83, 83, DATE '2025-03-24', 'Taxi', 94.79, 0.0, 94.79, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (84, 84, DATE '2025-03-25', 'Restaurant', 95.92, 0.0, 95.92, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'LOW', 'Client: auto', FALSE),
  (85, 85, DATE '2025-03-26', 'Airline', 97.05, 0.0, 97.05, 'USD', 'TRAVEL', 80, 'CASH', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (86, 86, DATE '2025-03-27', 'Office Depot', 98.18, 0.0, 98.18, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (87, 87, DATE '2025-03-28', 'Gas Station', 99.31, 0.0, 99.31, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (88, 88, DATE '2025-03-29', 'Hotel', 100.44, 0.0, 100.44, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', FALSE),
  (89, 89, DATE '2025-03-30', 'Cloud Vendor', 101.57, 0.0, 101.57, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (90, 90, DATE '2025-03-31', 'Taxi', 102.7, 0.0, 102.7, 'USD', 'MEAL', 50, 'CASH', 'CA-US', TRUE, 'PRJ-1001', 'HIGH', 'Client: auto', FALSE),
  (91, 91, DATE '2025-04-01', 'Restaurant', 103.83, 0.0, 103.83, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (92, 92, DATE '2025-04-02', 'Airline', 104.96, 0.0, 104.96, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'LOW', 'Client: auto', FALSE),
  (93, 93, DATE '2025-04-03', 'Office Depot', 106.09, 0.0, 106.09, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (94, 94, DATE '2025-04-04', 'Gas Station', 107.22, 0.0, 107.22, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'HIGH', 'Client: auto', FALSE),
  (95, 95, DATE '2025-04-05', 'Hotel', 108.35, 0.0, 108.35, 'USD', 'OTHER', 100, 'CASH', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (96, 96, DATE '2025-04-06', 'Cloud Vendor', 109.48, 0.0, 109.48, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'LOW', 'Client: auto', FALSE),
  (97, 97, DATE '2025-04-07', 'Taxi', 110.61, 0.0, 110.61, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (98, 98, DATE '2025-04-08', 'Restaurant', 111.74, 0.0, 111.74, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (99, 99, DATE '2025-04-09', 'Airline', 112.87, 0.0, 112.87, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (100, 100, DATE '2025-04-10', 'Office Depot', 114.0, 0.0, 114.0, 'USD', 'SOFTWARE', 100, 'CASH', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', FALSE)
) AS expected_expenses(
  expense_id, receipt_id, expense_date, vendor_name, net_amount, tax_amount, gross_amount,
  currency_code, expense_category, deductible_pct, payment_channel, jurisdiction, billable_flag,
  project_tag, documentation_quality, compliance_notes, cannot_categorize_safely
)""",
    1: """SELECT *
FROM (VALUES
  (1, 1, DATE '2025-01-01', 'Airline', 2.13, 0.0, 2.13, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (2, 2, DATE '2025-01-02', 'Office Depot', 3.26, 0.0, 3.26, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (3, 3, DATE '2025-01-03', 'Gas Station', 4.39, 0.0, 4.39, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (4, 4, DATE '2025-01-04', 'Hotel', 5.52, 0.0, 5.52, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', FALSE),
  (5, 5, DATE '2025-01-05', 'Cloud Vendor', 6.65, 0.0, 6.65, 'USD', 'OTHER', 100, 'CASH', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (6, 6, DATE '2025-01-06', 'Taxi', 7.78, 0.0, 7.78, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'HIGH', 'Client: auto', FALSE),
  (7, 7, DATE '2025-01-07', 'Restaurant', 8.91, 0.0, 8.91, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (8, 8, DATE '2025-01-08', 'Airline', 10.04, 0.0, 10.04, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'LOW', 'Client: auto', FALSE),
  (9, 9, DATE '2025-01-09', 'Office Depot', 11.17, 0.0, 11.17, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (10, 10, DATE '2025-01-10', 'Gas Station', 12.3, 0.0, 12.3, 'USD', 'SOFTWARE', 100, 'CASH', 'CA-US', TRUE, NULL, 'HIGH', 'Client: auto', FALSE),
  (11, 11, DATE '2025-01-11', 'Hotel', 13.43, 0.0, 13.43, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (12, 12, DATE '2025-01-12', 'Cloud Vendor', 14.56, 0.0, 14.56, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'LOW', 'Client: auto', FALSE),
  (13, 13, DATE '2025-01-13', 'Taxi', 15.69, 0.0, 15.69, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (14, 14, DATE '2025-01-14', 'Restaurant', 16.82, 0.0, 16.82, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (15, 15, DATE '2025-01-15', 'Airline', 17.95, 0.0, 17.95, 'USD', 'TRANSPORT', 100, 'CASH', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (16, 16, DATE '2025-01-16', 'Office Depot', 19.08, 0.0, 19.08, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', FALSE),
  (17, 17, DATE '2025-01-17', 'Gas Station', 20.21, 0.0, 20.21, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (18, 18, DATE '2025-01-18', 'Hotel', 21.34, 0.0, 21.34, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'HIGH', 'Client: auto', FALSE),
  (19, 19, DATE '2025-01-19', 'Cloud Vendor', 22.47, 0.0, 22.47, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (20, 20, DATE '2025-01-20', 'Taxi', 23.6, 0.0, 23.6, 'USD', 'OFFICE', 100, 'CASH', 'CA-US', TRUE, 'PRJ-2002', 'LOW', 'Client: auto', FALSE),
  (21, 21, DATE '2025-01-21', 'Restaurant', 24.73, 0.0, 24.73, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (22, 22, DATE '2025-01-22', 'Airline', 25.86, 0.0, 25.86, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'HIGH', 'Client: auto', FALSE),
  (23, 23, DATE '2025-01-23', 'Office Depot', 26.99, 0.0, 26.99, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (24, 24, DATE '2025-01-24', 'Gas Station', 28.12, 0.0, 28.12, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'LOW', 'Client: auto', FALSE),
  (25, 25, DATE '2025-01-25', 'Hotel', 29.25, 0.0, 29.25, 'USD', 'TRAVEL', 80, 'CASH', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (26, 26, DATE '2025-01-26', 'Cloud Vendor', 30.38, 0.0, 30.38, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (27, 27, DATE '2025-01-27', 'Taxi', 31.51, 0.0, 31.51, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (28, 28, DATE '2025-01-28', 'Restaurant', 32.64, 0.0, 32.64, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', FALSE),
  (29, 29, DATE '2025-01-29', 'Airline', 33.77, 0.0, 33.77, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (30, 30, DATE '2025-01-30', 'Office Depot', 34.9, 0.0, 34.9, 'USD', 'MEAL', 50, 'CASH', 'CA-US', TRUE, 'PRJ-1001', 'HIGH', 'Client: auto', FALSE),
  (31, 31, DATE '2025-01-31', 'Gas Station', 36.03, 0.0, 36.03, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (32, 32, DATE '2025-02-01', 'Hotel', 37.16, 0.0, 37.16, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'LOW', 'Client: auto', FALSE),
  (33, 33, DATE '2025-02-02', 'Cloud Vendor', 38.29, 0.0, 38.29, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (34, 34, DATE '2025-02-03', 'Taxi', 39.42, 0.0, 39.42, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'HIGH', 'Client: auto', FALSE),
  (35, 35, DATE '2025-02-04', 'Restaurant', 40.55, 0.0, 40.55, 'USD', 'OTHER', 100, 'CASH', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (36, 36, DATE '2025-02-05', 'Airline', 41.68, 0.0, 41.68, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'LOW', 'Client: auto', FALSE),
  (37, 37, DATE '2025-02-06', 'Office Depot', 42.81, 0.0, 42.81, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (38, 38, DATE '2025-02-07', 'Gas Station', 43.94, 0.0, 43.94, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (39, 39, DATE '2025-02-08', 'Hotel', 45.07, 0.0, 45.07, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (40, 40, DATE '2025-02-09', 'Cloud Vendor', 46.2, 0.0, 46.2, 'USD', 'SOFTWARE', 100, 'CASH', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', FALSE),
  (41, 41, DATE '2025-02-10', 'Taxi', 47.33, 0.0, 47.33, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (42, 42, DATE '2025-02-11', 'Restaurant', 48.46, 0.0, 48.46, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'HIGH', 'Client: auto', FALSE),
  (43, 43, DATE '2025-02-12', 'Airline', 49.59, 0.0, 49.59, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (44, 44, DATE '2025-02-13', 'Office Depot', 50.72, 0.0, 50.72, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'LOW', 'Client: auto', FALSE),
  (45, 45, DATE '2025-02-14', 'Gas Station', 51.85, 0.0, 51.85, 'USD', 'TRANSPORT', 100, 'CASH', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (46, 46, DATE '2025-02-15', 'Hotel', 52.98, 0.0, 52.98, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'HIGH', 'Client: auto', FALSE),
  (47, 47, DATE '2025-02-16', 'Cloud Vendor', 54.11, 0.0, 54.11, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (48, 48, DATE '2025-02-17', 'Taxi', 55.24, 0.0, 55.24, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'LOW', 'Client: auto', FALSE),
  (49, 49, DATE '2025-02-18', 'Restaurant', 56.37, 0.0, 56.37, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (50, 50, DATE '2025-02-19', 'Airline', 57.5, 0.0, 57.5, 'USD', 'OFFICE', 100, 'CASH', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (51, 51, DATE '2025-02-20', 'Office Depot', 58.63, 0.0, 58.63, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (52, 52, DATE '2025-02-21', 'Gas Station', 59.76, 0.0, 59.76, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', FALSE),
  (53, 53, DATE '2025-02-22', 'Hotel', 60.89, 0.0, 60.89, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (54, 54, DATE '2025-02-23', 'Cloud Vendor', 62.02, 0.0, 62.02, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'HIGH', 'Client: auto', FALSE),
  (55, 55, DATE '2025-02-24', 'Taxi', 63.15, 0.0, 63.15, 'USD', 'TRAVEL', 80, 'CASH', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (56, 56, DATE '2025-02-25', 'Restaurant', 64.28, 0.0, 64.28, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'LOW', 'Client: auto', FALSE),
  (57, 57, DATE '2025-02-26', 'Airline', 65.41, 0.0, 65.41, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (58, 58, DATE '2025-02-27', 'Office Depot', 66.54, 0.0, 66.54, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'HIGH', 'Client: auto', FALSE),
  (59, 59, DATE '2025-02-28', 'Gas Station', 67.67, 0.0, 67.67, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (60, 60, DATE '2025-03-01', 'Hotel', 68.8, 0.0, 68.8, 'USD', 'MEAL', 50, 'CASH', 'CA-US', TRUE, 'PRJ-1001', 'LOW', 'Client: auto', FALSE),
  (61, 61, DATE '2025-03-02', 'Cloud Vendor', 69.93, 0.0, 69.93, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (62, 62, DATE '2025-03-03', 'Taxi', 71.06, 0.0, 71.06, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (63, 63, DATE '2025-03-04', 'Restaurant', 72.19, 0.0, 72.19, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (64, 64, DATE '2025-03-05', 'Airline', 73.32, 0.0, 73.32, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', FALSE),
  (65, 65, DATE '2025-03-06', 'Office Depot', 74.45, 0.0, 74.45, 'USD', 'OTHER', 100, 'CASH', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (66, 66, DATE '2025-03-07', 'Gas Station', 75.58, 0.0, 75.58, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'HIGH', 'Client: auto', FALSE),
  (67, 67, DATE '2025-03-08', 'Hotel', 76.71, 0.0, 76.71, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (68, 68, DATE '2025-03-09', 'Cloud Vendor', 77.84, 0.0, 77.84, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'LOW', 'Client: auto', FALSE),
  (69, 69, DATE '2025-03-10', 'Taxi', 78.97, 0.0, 78.97, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (70, 70, DATE '2025-03-11', 'Restaurant', 80.1, 0.0, 80.1, 'USD', 'SOFTWARE', 100, 'CASH', 'CA-US', TRUE, NULL, 'HIGH', 'Client: auto', FALSE),
  (71, 71, DATE '2025-03-12', 'Airline', 81.23, 0.0, 81.23, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (72, 72, DATE '2025-03-13', 'Office Depot', 82.36, 0.0, 82.36, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'LOW', 'Client: auto', FALSE),
  (73, 73, DATE '2025-03-14', 'Gas Station', 83.49, 0.0, 83.49, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (74, 74, DATE '2025-03-15', 'Hotel', 84.62, 0.0, 84.62, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (75, 75, DATE '2025-03-16', 'Cloud Vendor', 85.75, 0.0, 85.75, 'USD', 'TRANSPORT', 100, 'CASH', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (76, 76, DATE '2025-03-17', 'Taxi', 86.88, 0.0, 86.88, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', FALSE),
  (77, 77, DATE '2025-03-18', 'Restaurant', 88.01, 0.0, 88.01, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (78, 78, DATE '2025-03-19', 'Airline', 89.14, 0.0, 89.14, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'HIGH', 'Client: auto', FALSE),
  (79, 79, DATE '2025-03-20', 'Office Depot', 90.27, 0.0, 90.27, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (80, 80, DATE '2025-03-21', 'Gas Station', 91.4, 0.0, 91.4, 'USD', 'OFFICE', 100, 'CASH', 'CA-US', TRUE, 'PRJ-2002', 'LOW', 'Client: auto', FALSE),
  (81, 81, DATE '2025-03-22', 'Hotel', 92.53, 0.0, 92.53, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (82, 82, DATE '2025-03-23', 'Cloud Vendor', 93.66, 0.0, 93.66, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'HIGH', 'Client: auto', FALSE),
  (83, 83, DATE '2025-03-24', 'Taxi', 94.79, 0.0, 94.79, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (84, 84, DATE '2025-03-25', 'Restaurant', 95.92, 0.0, 95.92, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'LOW', 'Client: auto', FALSE),
  (85, 85, DATE '2025-03-26', 'Airline', 97.05, 0.0, 97.05, 'USD', 'TRAVEL', 80, 'CASH', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (86, 86, DATE '2025-03-27', 'Office Depot', 98.18, 0.0, 98.18, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (87, 87, DATE '2025-03-28', 'Gas Station', 99.31, 0.0, 99.31, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (88, 88, DATE '2025-03-29', 'Hotel', 100.44, 0.0, 100.44, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', FALSE),
  (89, 89, DATE '2025-03-30', 'Cloud Vendor', 101.57, 0.0, 101.57, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (90, 90, DATE '2025-03-31', 'Taxi', 102.7, 0.0, 102.7, 'USD', 'MEAL', 50, 'CASH', 'CA-US', TRUE, 'PRJ-1001', 'HIGH', 'Client: auto', FALSE),
  (91, 91, DATE '2025-04-01', 'Restaurant', 103.83, 0.0, 103.83, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (92, 92, DATE '2025-04-02', 'Airline', 104.96, 0.0, 104.96, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'LOW', 'Client: auto', FALSE),
  (93, 93, DATE '2025-04-03', 'Office Depot', 106.09, 0.0, 106.09, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (94, 94, DATE '2025-04-04', 'Gas Station', 107.22, 0.0, 107.22, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'HIGH', 'Client: auto', FALSE),
  (95, 95, DATE '2025-04-05', 'Hotel', 108.35, 0.0, 108.35, 'USD', 'OTHER', 100, 'CASH', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (96, 96, DATE '2025-04-06', 'Cloud Vendor', 109.48, 0.0, 109.48, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'LOW', 'Client: auto', FALSE),
  (97, 97, DATE '2025-04-07', 'Taxi', 110.61, 0.0, 110.61, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (98, 98, DATE '2025-04-08', 'Restaurant', 111.74, 0.0, 111.74, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (99, 99, DATE '2025-04-09', 'Airline', 112.87, 0.0, 112.87, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (100, 100, DATE '2025-04-10', 'Office Depot', 114.0, 0.0, 114.0, 'USD', 'SOFTWARE', 100, 'CASH', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', FALSE)
) AS expected_expenses(
  expense_id, receipt_id, expense_date, vendor_name, net_amount, tax_amount, gross_amount,
  currency_code, expense_category, deductible_pct, payment_channel, jurisdiction, billable_flag,
  project_tag, documentation_quality, compliance_notes, cannot_categorize_safely
)""",
    2: """SELECT *
FROM (VALUES
  (1, 1, DATE '2025-01-01', 'Airline', 2.13, 0.0, 2.13, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (2, 2, DATE '2025-01-02', 'Office Depot', 3.26, 0.0, 3.26, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (3, 3, DATE '2025-01-03', 'Gas Station', 4.39, 0.0, 4.39, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (4, 4, DATE '2025-01-04', 'Hotel', 5.52, 0.0, 5.52, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', FALSE),
  (5, 5, DATE '2025-01-05', 'Cloud Vendor', 6.65, 0.0, 6.65, 'USD', 'OTHER', 100, 'CASH', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (6, 6, DATE '2025-01-06', 'Taxi', 7.78, 0.0, 7.78, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'HIGH', 'Client: auto', FALSE),
  (7, 7, DATE '2025-01-07', 'Restaurant', 8.91, 0.0, 8.91, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (8, 8, DATE '2025-01-08', 'Airline', 10.04, 0.0, 10.04, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'LOW', 'Client: auto', FALSE),
  (9, 9, DATE '2025-01-09', 'Office Depot', 11.17, 0.0, 11.17, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (10, 10, DATE '2025-01-10', 'Gas Station', 12.3, 0.0, 12.3, 'USD', 'SOFTWARE', 100, 'CASH', 'CA-US', TRUE, NULL, 'HIGH', 'Client: auto', FALSE),
  (11, 11, DATE '2025-01-11', 'Hotel', 13.43, 0.0, 13.43, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (12, 12, DATE '2025-01-12', 'Cloud Vendor', 14.56, 0.0, 14.56, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'LOW', 'Client: auto', FALSE),
  (13, 13, DATE '2025-01-13', 'Taxi', 15.69, 0.0, 15.69, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (14, 14, DATE '2025-01-14', 'Restaurant', 16.82, 0.0, 16.82, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (15, 15, DATE '2025-01-15', 'Airline', 17.95, 0.0, 17.95, 'USD', 'TRANSPORT', 100, 'CASH', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (16, 16, DATE '2025-01-16', 'Office Depot', 19.08, 0.0, 19.08, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', FALSE),
  (17, 17, DATE '2025-01-17', 'Gas Station', 20.21, 0.0, 20.21, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (18, 18, DATE '2025-01-18', 'Hotel', 21.34, 0.0, 21.34, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'HIGH', 'Client: auto', FALSE),
  (19, 19, DATE '2025-01-19', 'Cloud Vendor', 22.47, 0.0, 22.47, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (20, 20, DATE '2025-01-20', 'Taxi', 23.6, 0.0, 23.6, 'USD', 'OFFICE', 100, 'CASH', 'CA-US', TRUE, 'PRJ-2002', 'LOW', 'Client: auto', FALSE),
  (21, 21, DATE '2025-01-21', 'Restaurant', 24.73, 0.0, 24.73, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (22, 22, DATE '2025-01-22', 'Airline', 25.86, 0.0, 25.86, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'HIGH', 'Client: auto', FALSE),
  (23, 23, DATE '2025-01-23', 'Office Depot', 26.99, 0.0, 26.99, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (24, 24, DATE '2025-01-24', 'Gas Station', 28.12, 0.0, 28.12, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'LOW', 'Client: auto', FALSE),
  (25, 25, DATE '2025-01-25', 'Hotel', 29.25, 0.0, 29.25, 'USD', 'TRAVEL', 80, 'CASH', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (26, 26, DATE '2025-01-26', 'Cloud Vendor', 30.38, 0.0, 30.38, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (27, 27, DATE '2025-01-27', 'Taxi', 31.51, 0.0, 31.51, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (28, 28, DATE '2025-01-28', 'Restaurant', 32.64, 0.0, 32.64, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', FALSE),
  (29, 29, DATE '2025-01-29', 'Airline', 33.77, 0.0, 33.77, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (30, 30, DATE '2025-01-30', 'Office Depot', 34.9, 0.0, 34.9, 'USD', 'MEAL', 50, 'CASH', 'CA-US', TRUE, 'PRJ-1001', 'HIGH', 'Client: auto', FALSE),
  (31, 31, DATE '2025-01-31', 'Gas Station', 36.03, 0.0, 36.03, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (32, 32, DATE '2025-02-01', 'Hotel', 37.16, 0.0, 37.16, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'LOW', 'Client: auto', FALSE),
  (33, 33, DATE '2025-02-02', 'Cloud Vendor', 38.29, 0.0, 38.29, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (34, 34, DATE '2025-02-03', 'Taxi', 39.42, 0.0, 39.42, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'HIGH', 'Client: auto', FALSE),
  (35, 35, DATE '2025-02-04', 'Restaurant', 40.55, 0.0, 40.55, 'USD', 'OTHER', 100, 'CASH', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (36, 36, DATE '2025-02-05', 'Airline', 41.68, 0.0, 41.68, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'LOW', 'Client: auto', FALSE),
  (37, 37, DATE '2025-02-06', 'Office Depot', 42.81, 0.0, 42.81, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (38, 38, DATE '2025-02-07', 'Gas Station', 43.94, 0.0, 43.94, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (39, 39, DATE '2025-02-08', 'Hotel', 45.07, 0.0, 45.07, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (40, 40, DATE '2025-02-09', 'Cloud Vendor', 46.2, 0.0, 46.2, 'USD', 'SOFTWARE', 100, 'CASH', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', FALSE),
  (41, 41, DATE '2025-02-10', 'Taxi', 47.33, 0.0, 47.33, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (42, 42, DATE '2025-02-11', 'Restaurant', 48.46, 0.0, 48.46, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'HIGH', 'Client: auto', FALSE),
  (43, 43, DATE '2025-02-12', 'Airline', 49.59, 0.0, 49.59, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (44, 44, DATE '2025-02-13', 'Office Depot', 50.72, 0.0, 50.72, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'LOW', 'Client: auto', FALSE),
  (45, 45, DATE '2025-02-14', 'Gas Station', 51.85, 0.0, 51.85, 'USD', 'TRANSPORT', 100, 'CASH', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (46, 46, DATE '2025-02-15', 'Hotel', 52.98, 0.0, 52.98, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'HIGH', 'Client: auto', FALSE),
  (47, 47, DATE '2025-02-16', 'Cloud Vendor', 54.11, 0.0, 54.11, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (48, 48, DATE '2025-02-17', 'Taxi', 55.24, 0.0, 55.24, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'LOW', 'Client: auto', FALSE),
  (49, 49, DATE '2025-02-18', 'Restaurant', 56.37, 0.0, 56.37, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (50, 50, DATE '2025-02-19', 'Airline', 57.5, 0.0, 57.5, 'USD', 'OFFICE', 100, 'CASH', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (51, 51, DATE '2025-02-20', 'Office Depot', 58.63, 0.0, 58.63, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (52, 52, DATE '2025-02-21', 'Gas Station', 59.76, 0.0, 59.76, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', FALSE),
  (53, 53, DATE '2025-02-22', 'Hotel', 60.89, 0.0, 60.89, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (54, 54, DATE '2025-02-23', 'Cloud Vendor', 62.02, 0.0, 62.02, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'HIGH', 'Client: auto', FALSE),
  (55, 55, DATE '2025-02-24', 'Taxi', 63.15, 0.0, 63.15, 'USD', 'TRAVEL', 80, 'CASH', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (56, 56, DATE '2025-02-25', 'Restaurant', 64.28, 0.0, 64.28, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'LOW', 'Client: auto', FALSE),
  (57, 57, DATE '2025-02-26', 'Airline', 65.41, 0.0, 65.41, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (58, 58, DATE '2025-02-27', 'Office Depot', 66.54, 0.0, 66.54, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'HIGH', 'Client: auto', FALSE),
  (59, 59, DATE '2025-02-28', 'Gas Station', 67.67, 0.0, 67.67, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (60, 60, DATE '2025-03-01', 'Hotel', 68.8, 0.0, 68.8, 'USD', 'MEAL', 50, 'CASH', 'CA-US', TRUE, 'PRJ-1001', 'LOW', 'Client: auto', FALSE),
  (61, 61, DATE '2025-03-02', 'Cloud Vendor', 69.93, 0.0, 69.93, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (62, 62, DATE '2025-03-03', 'Taxi', 71.06, 0.0, 71.06, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (63, 63, DATE '2025-03-04', 'Restaurant', 72.19, 0.0, 72.19, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (64, 64, DATE '2025-03-05', 'Airline', 73.32, 0.0, 73.32, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', FALSE),
  (65, 65, DATE '2025-03-06', 'Office Depot', 74.45, 0.0, 74.45, 'USD', 'OTHER', 100, 'CASH', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (66, 66, DATE '2025-03-07', 'Gas Station', 75.58, 0.0, 75.58, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'HIGH', 'Client: auto', FALSE),
  (67, 67, DATE '2025-03-08', 'Hotel', 76.71, 0.0, 76.71, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (68, 68, DATE '2025-03-09', 'Cloud Vendor', 77.84, 0.0, 77.84, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'LOW', 'Client: auto', FALSE),
  (69, 69, DATE '2025-03-10', 'Taxi', 78.97, 0.0, 78.97, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (70, 70, DATE '2025-03-11', 'Restaurant', 80.1, 0.0, 80.1, 'USD', 'SOFTWARE', 100, 'CASH', 'CA-US', TRUE, NULL, 'HIGH', 'Client: auto', FALSE),
  (71, 71, DATE '2025-03-12', 'Airline', 81.23, 0.0, 81.23, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (72, 72, DATE '2025-03-13', 'Office Depot', 82.36, 0.0, 82.36, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'LOW', 'Client: auto', FALSE),
  (73, 73, DATE '2025-03-14', 'Gas Station', 83.49, 0.0, 83.49, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (74, 74, DATE '2025-03-15', 'Hotel', 84.62, 0.0, 84.62, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (75, 75, DATE '2025-03-16', 'Cloud Vendor', 85.75, 0.0, 85.75, 'USD', 'TRANSPORT', 100, 'CASH', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (76, 76, DATE '2025-03-17', 'Taxi', 86.88, 0.0, 86.88, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', FALSE),
  (77, 77, DATE '2025-03-18', 'Restaurant', 88.01, 0.0, 88.01, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (78, 78, DATE '2025-03-19', 'Airline', 89.14, 0.0, 89.14, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'HIGH', 'Client: auto', FALSE),
  (79, 79, DATE '2025-03-20', 'Office Depot', 90.27, 0.0, 90.27, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (80, 80, DATE '2025-03-21', 'Gas Station', 91.4, 0.0, 91.4, 'USD', 'OFFICE', 100, 'CASH', 'CA-US', TRUE, 'PRJ-2002', 'LOW', 'Client: auto', FALSE),
  (81, 81, DATE '2025-03-22', 'Hotel', 92.53, 0.0, 92.53, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (82, 82, DATE '2025-03-23', 'Cloud Vendor', 93.66, 0.0, 93.66, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'HIGH', 'Client: auto', FALSE),
  (83, 83, DATE '2025-03-24', 'Taxi', 94.79, 0.0, 94.79, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (84, 84, DATE '2025-03-25', 'Restaurant', 95.92, 0.0, 95.92, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'LOW', 'Client: auto', FALSE),
  (85, 85, DATE '2025-03-26', 'Airline', 97.05, 0.0, 97.05, 'USD', 'TRAVEL', 80, 'CASH', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (86, 86, DATE '2025-03-27', 'Office Depot', 98.18, 0.0, 98.18, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (87, 87, DATE '2025-03-28', 'Gas Station', 99.31, 0.0, 99.31, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (88, 88, DATE '2025-03-29', 'Hotel', 100.44, 0.0, 100.44, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', FALSE),
  (89, 89, DATE '2025-03-30', 'Cloud Vendor', 101.57, 0.0, 101.57, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (90, 90, DATE '2025-03-31', 'Taxi', 102.7, 0.0, 102.7, 'USD', 'MEAL', 50, 'CASH', 'CA-US', TRUE, 'PRJ-1001', 'HIGH', 'Client: auto', FALSE),
  (91, 91, DATE '2025-04-01', 'Restaurant', 103.83, 0.0, 103.83, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (92, 92, DATE '2025-04-02', 'Airline', 104.96, 0.0, 104.96, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'LOW', 'Client: auto', FALSE),
  (93, 93, DATE '2025-04-03', 'Office Depot', 106.09, 0.0, 106.09, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (94, 94, DATE '2025-04-04', 'Gas Station', 107.22, 0.0, 107.22, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'HIGH', 'Client: auto', FALSE),
  (95, 95, DATE '2025-04-05', 'Hotel', 108.35, 0.0, 108.35, 'USD', 'OTHER', 100, 'CASH', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (96, 96, DATE '2025-04-06', 'Cloud Vendor', 109.48, 0.0, 109.48, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'LOW', 'Client: auto', FALSE),
  (97, 97, DATE '2025-04-07', 'Taxi', 110.61, 0.0, 110.61, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (98, 98, DATE '2025-04-08', 'Restaurant', 111.74, 0.0, 111.74, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (99, 99, DATE '2025-04-09', 'Airline', 112.87, 0.0, 112.87, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (100, 100, DATE '2025-04-10', 'Office Depot', 114.0, 0.0, 114.0, 'USD', 'SOFTWARE', 100, 'CASH', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', FALSE)
) AS expected_expenses(
  expense_id, receipt_id, expense_date, vendor_name, net_amount, tax_amount, gross_amount,
  currency_code, expense_category, deductible_pct, payment_channel, jurisdiction, billable_flag,
  project_tag, documentation_quality, compliance_notes, cannot_categorize_safely
)""",
    4: """SELECT *
FROM (VALUES
  (1, 1, DATE '2025-01-01', 'Airline', 2.13, 0.0, 2.13, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (2, 2, DATE '2025-01-02', 'Office Depot', 3.26, 0.0, 3.26, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (3, 3, DATE '2025-01-03', 'Gas Station', 4.39, 0.0, 4.39, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (4, 4, DATE '2025-01-04', 'Hotel', 5.52, 0.0, 5.52, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', FALSE),
  (5, 5, DATE '2025-01-05', 'Cloud Vendor', 6.65, 0.0, 6.65, 'USD', 'OTHER', 100, 'CASH', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (6, 6, DATE '2025-01-06', 'Taxi', 7.78, 0.0, 7.78, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'HIGH', 'Client: auto', FALSE),
  (7, 7, DATE '2025-01-07', 'Restaurant', 8.91, 0.0, 8.91, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (8, 8, DATE '2025-01-08', 'Airline', 10.04, 0.0, 10.04, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'LOW', 'Client: auto', FALSE),
  (9, 9, DATE '2025-01-09', 'Office Depot', 11.17, 0.0, 11.17, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (10, 10, DATE '2025-01-10', 'Gas Station', 12.3, 0.0, 12.3, 'USD', 'SOFTWARE', 100, 'CASH', 'CA-US', TRUE, NULL, 'HIGH', 'Client: auto', FALSE),
  (11, 11, DATE '2025-01-11', 'Hotel', 13.43, 0.0, 13.43, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (12, 12, DATE '2025-01-12', 'Cloud Vendor', 14.56, 0.0, 14.56, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'LOW', 'Client: auto', FALSE),
  (13, 13, DATE '2025-01-13', 'Taxi', 15.69, 0.0, 15.69, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (14, 14, DATE '2025-01-14', 'Restaurant', 16.82, 0.0, 16.82, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (15, 15, DATE '2025-01-15', 'Airline', 17.95, 0.0, 17.95, 'USD', 'TRANSPORT', 100, 'CASH', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (16, 16, DATE '2025-01-16', 'Office Depot', 19.08, 0.0, 19.08, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', FALSE),
  (17, 17, DATE '2025-01-17', 'Gas Station', 20.21, 0.0, 20.21, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (18, 18, DATE '2025-01-18', 'Hotel', 21.34, 0.0, 21.34, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'HIGH', 'Client: auto', FALSE),
  (19, 19, DATE '2025-01-19', 'Cloud Vendor', 22.47, 0.0, 22.47, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (20, 20, DATE '2025-01-20', 'Taxi', 23.6, 0.0, 23.6, 'USD', 'OFFICE', 100, 'CASH', 'CA-US', TRUE, 'PRJ-2002', 'LOW', 'Client: auto', FALSE),
  (21, 21, DATE '2025-01-21', 'Restaurant', 24.73, 0.0, 24.73, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (22, 22, DATE '2025-01-22', 'Airline', 25.86, 0.0, 25.86, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'HIGH', 'Client: auto', FALSE),
  (23, 23, DATE '2025-01-23', 'Office Depot', 26.99, 0.0, 26.99, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (24, 24, DATE '2025-01-24', 'Gas Station', 28.12, 0.0, 28.12, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'LOW', 'Client: auto', FALSE),
  (25, 25, DATE '2025-01-25', 'Hotel', 29.25, 0.0, 29.25, 'USD', 'TRAVEL', 80, 'CASH', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (26, 26, DATE '2025-01-26', 'Cloud Vendor', 30.38, 0.0, 30.38, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (27, 27, DATE '2025-01-27', 'Taxi', 31.51, 0.0, 31.51, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (28, 28, DATE '2025-01-28', 'Restaurant', 32.64, 0.0, 32.64, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', FALSE),
  (29, 29, DATE '2025-01-29', 'Airline', 33.77, 0.0, 33.77, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (30, 30, DATE '2025-01-30', 'Office Depot', 34.9, 0.0, 34.9, 'USD', 'MEAL', 50, 'CASH', 'CA-US', TRUE, 'PRJ-1001', 'HIGH', 'Client: auto', FALSE),
  (31, 31, DATE '2025-01-31', 'Gas Station', 36.03, 0.0, 36.03, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (32, 32, DATE '2025-02-01', 'Hotel', 37.16, 0.0, 37.16, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'LOW', 'Client: auto', FALSE),
  (33, 33, DATE '2025-02-02', 'Cloud Vendor', 38.29, 0.0, 38.29, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (34, 34, DATE '2025-02-03', 'Taxi', 39.42, 0.0, 39.42, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'HIGH', 'Client: auto', FALSE),
  (35, 35, DATE '2025-02-04', 'Restaurant', 40.55, 0.0, 40.55, 'USD', 'OTHER', 100, 'CASH', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (36, 36, DATE '2025-02-05', 'Airline', 41.68, 0.0, 41.68, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'LOW', 'Client: auto', FALSE),
  (37, 37, DATE '2025-02-06', 'Office Depot', 42.81, 0.0, 42.81, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (38, 38, DATE '2025-02-07', 'Gas Station', 43.94, 0.0, 43.94, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (39, 39, DATE '2025-02-08', 'Hotel', 45.07, 0.0, 45.07, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (40, 40, DATE '2025-02-09', 'Cloud Vendor', 46.2, 0.0, 46.2, 'USD', 'SOFTWARE', 100, 'CASH', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', FALSE),
  (41, 41, DATE '2025-02-10', 'Taxi', 47.33, 0.0, 47.33, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (42, 42, DATE '2025-02-11', 'Restaurant', 48.46, 0.0, 48.46, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'HIGH', 'Client: auto', FALSE),
  (43, 43, DATE '2025-02-12', 'Airline', 49.59, 0.0, 49.59, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (44, 44, DATE '2025-02-13', 'Office Depot', 50.72, 0.0, 50.72, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'LOW', 'Client: auto', FALSE),
  (45, 45, DATE '2025-02-14', 'Gas Station', 51.85, 0.0, 51.85, 'USD', 'TRANSPORT', 100, 'CASH', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (46, 46, DATE '2025-02-15', 'Hotel', 52.98, 0.0, 52.98, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'HIGH', 'Client: auto', FALSE),
  (47, 47, DATE '2025-02-16', 'Cloud Vendor', 54.11, 0.0, 54.11, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (48, 48, DATE '2025-02-17', 'Taxi', 55.24, 0.0, 55.24, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'LOW', 'Client: auto', FALSE),
  (49, 49, DATE '2025-02-18', 'Restaurant', 56.37, 0.0, 56.37, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (50, 50, DATE '2025-02-19', 'Airline', 57.5, 0.0, 57.5, 'USD', 'OFFICE', 100, 'CASH', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (51, 51, DATE '2025-02-20', 'Office Depot', 58.63, 0.0, 58.63, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (52, 52, DATE '2025-02-21', 'Gas Station', 59.76, 0.0, 59.76, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', FALSE),
  (53, 53, DATE '2025-02-22', 'Hotel', 60.89, 0.0, 60.89, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (54, 54, DATE '2025-02-23', 'Cloud Vendor', 62.02, 0.0, 62.02, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'HIGH', 'Client: auto', FALSE),
  (55, 55, DATE '2025-02-24', 'Taxi', 63.15, 0.0, 63.15, 'USD', 'TRAVEL', 80, 'CASH', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (56, 56, DATE '2025-02-25', 'Restaurant', 64.28, 0.0, 64.28, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'LOW', 'Client: auto', FALSE),
  (57, 57, DATE '2025-02-26', 'Airline', 65.41, 0.0, 65.41, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (58, 58, DATE '2025-02-27', 'Office Depot', 66.54, 0.0, 66.54, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'HIGH', 'Client: auto', FALSE),
  (59, 59, DATE '2025-02-28', 'Gas Station', 67.67, 0.0, 67.67, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (60, 60, DATE '2025-03-01', 'Hotel', 68.8, 0.0, 68.8, 'USD', 'MEAL', 50, 'CASH', 'CA-US', TRUE, 'PRJ-1001', 'LOW', 'Client: auto', FALSE),
  (61, 61, DATE '2025-03-02', 'Cloud Vendor', 69.93, 0.0, 69.93, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (62, 62, DATE '2025-03-03', 'Taxi', 71.06, 0.0, 71.06, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (63, 63, DATE '2025-03-04', 'Restaurant', 72.19, 0.0, 72.19, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (64, 64, DATE '2025-03-05', 'Airline', 73.32, 0.0, 73.32, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', FALSE),
  (65, 65, DATE '2025-03-06', 'Office Depot', 74.45, 0.0, 74.45, 'USD', 'OTHER', 100, 'CASH', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (66, 66, DATE '2025-03-07', 'Gas Station', 75.58, 0.0, 75.58, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'HIGH', 'Client: auto', FALSE),
  (67, 67, DATE '2025-03-08', 'Hotel', 76.71, 0.0, 76.71, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (68, 68, DATE '2025-03-09', 'Cloud Vendor', 77.84, 0.0, 77.84, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'LOW', 'Client: auto', FALSE),
  (69, 69, DATE '2025-03-10', 'Taxi', 78.97, 0.0, 78.97, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (70, 70, DATE '2025-03-11', 'Restaurant', 80.1, 0.0, 80.1, 'USD', 'SOFTWARE', 100, 'CASH', 'CA-US', TRUE, NULL, 'HIGH', 'Client: auto', FALSE),
  (71, 71, DATE '2025-03-12', 'Airline', 81.23, 0.0, 81.23, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (72, 72, DATE '2025-03-13', 'Office Depot', 82.36, 0.0, 82.36, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'LOW', 'Client: auto', FALSE),
  (73, 73, DATE '2025-03-14', 'Gas Station', 83.49, 0.0, 83.49, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (74, 74, DATE '2025-03-15', 'Hotel', 84.62, 0.0, 84.62, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (75, 75, DATE '2025-03-16', 'Cloud Vendor', 85.75, 0.0, 85.75, 'USD', 'TRANSPORT', 100, 'CASH', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (76, 76, DATE '2025-03-17', 'Taxi', 86.88, 0.0, 86.88, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', FALSE),
  (77, 77, DATE '2025-03-18', 'Restaurant', 88.01, 0.0, 88.01, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (78, 78, DATE '2025-03-19', 'Airline', 89.14, 0.0, 89.14, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'HIGH', 'Client: auto', FALSE),
  (79, 79, DATE '2025-03-20', 'Office Depot', 90.27, 0.0, 90.27, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (80, 80, DATE '2025-03-21', 'Gas Station', 91.4, 0.0, 91.4, 'USD', 'OFFICE', 100, 'CASH', 'CA-US', TRUE, 'PRJ-2002', 'LOW', 'Client: auto', FALSE),
  (81, 81, DATE '2025-03-22', 'Hotel', 92.53, 0.0, 92.53, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (82, 82, DATE '2025-03-23', 'Cloud Vendor', 93.66, 0.0, 93.66, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'HIGH', 'Client: auto', FALSE),
  (83, 83, DATE '2025-03-24', 'Taxi', 94.79, 0.0, 94.79, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (84, 84, DATE '2025-03-25', 'Restaurant', 95.92, 0.0, 95.92, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'LOW', 'Client: auto', FALSE),
  (85, 85, DATE '2025-03-26', 'Airline', 97.05, 0.0, 97.05, 'USD', 'TRAVEL', 80, 'CASH', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (86, 86, DATE '2025-03-27', 'Office Depot', 98.18, 0.0, 98.18, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (87, 87, DATE '2025-03-28', 'Gas Station', 99.31, 0.0, 99.31, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (88, 88, DATE '2025-03-29', 'Hotel', 100.44, 0.0, 100.44, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', FALSE),
  (89, 89, DATE '2025-03-30', 'Cloud Vendor', 101.57, 0.0, 101.57, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (90, 90, DATE '2025-03-31', 'Taxi', 102.7, 0.0, 102.7, 'USD', 'MEAL', 50, 'CASH', 'CA-US', TRUE, 'PRJ-1001', 'HIGH', 'Client: auto', FALSE),
  (91, 91, DATE '2025-04-01', 'Restaurant', 103.83, 0.0, 103.83, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (92, 92, DATE '2025-04-02', 'Airline', 104.96, 0.0, 104.96, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'LOW', 'Client: auto', FALSE),
  (93, 93, DATE '2025-04-03', 'Office Depot', 106.09, 0.0, 106.09, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (94, 94, DATE '2025-04-04', 'Gas Station', 107.22, 0.0, 107.22, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'HIGH', 'Client: auto', FALSE),
  (95, 95, DATE '2025-04-05', 'Hotel', 108.35, 0.0, 108.35, 'USD', 'OTHER', 100, 'CASH', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (96, 96, DATE '2025-04-06', 'Cloud Vendor', 109.48, 0.0, 109.48, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'LOW', 'Client: auto', FALSE),
  (97, 97, DATE '2025-04-07', 'Taxi', 110.61, 0.0, 110.61, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (98, 98, DATE '2025-04-08', 'Restaurant', 111.74, 0.0, 111.74, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (99, 99, DATE '2025-04-09', 'Airline', 112.87, 0.0, 112.87, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (100, 100, DATE '2025-04-10', 'Office Depot', 114.0, 0.0, 114.0, 'USD', 'SOFTWARE', 100, 'CASH', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', FALSE)
) AS expected_expenses(
  expense_id, receipt_id, expense_date, vendor_name, net_amount, tax_amount, gross_amount,
  currency_code, expense_category, deductible_pct, payment_channel, jurisdiction, billable_flag,
  project_tag, documentation_quality, compliance_notes, cannot_categorize_safely
)""",
    8: """SELECT *
FROM (VALUES
  (1, 1, DATE '2025-01-01', 'Airline', 2.13, 0.0, 2.13, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (2, 2, DATE '2025-01-02', 'Office Depot', 3.26, 0.0, 3.26, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (3, 3, DATE '2025-01-03', 'Gas Station', 4.39, 0.0, 4.39, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (4, 4, DATE '2025-01-04', 'Hotel', 5.52, 0.0, 5.52, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', FALSE),
  (5, 5, DATE '2025-01-05', 'Cloud Vendor', 6.65, 0.0, 6.65, 'USD', 'OTHER', 100, 'CASH', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (6, 6, DATE '2025-01-06', 'Taxi', 7.78, 0.0, 7.78, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'HIGH', 'Client: auto', FALSE),
  (7, 7, DATE '2025-01-07', 'Restaurant', 8.91, 0.0, 8.91, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (8, 8, DATE '2025-01-08', 'Airline', 10.04, 0.0, 10.04, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'LOW', 'Client: auto', FALSE),
  (9, 9, DATE '2025-01-09', 'Office Depot', 11.17, 0.0, 11.17, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (10, 10, DATE '2025-01-10', 'Gas Station', 12.3, 0.0, 12.3, 'USD', 'SOFTWARE', 100, 'CASH', 'CA-US', TRUE, NULL, 'HIGH', 'Client: auto', FALSE),
  (11, 11, DATE '2025-01-11', 'Hotel', 13.43, 0.0, 13.43, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (12, 12, DATE '2025-01-12', 'Cloud Vendor', 14.56, 0.0, 14.56, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'LOW', 'Client: auto', FALSE),
  (13, 13, DATE '2025-01-13', 'Taxi', 15.69, 0.0, 15.69, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (14, 14, DATE '2025-01-14', 'Restaurant', 16.82, 0.0, 16.82, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (15, 15, DATE '2025-01-15', 'Airline', 17.95, 0.0, 17.95, 'USD', 'TRANSPORT', 100, 'CASH', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (16, 16, DATE '2025-01-16', 'Office Depot', 19.08, 0.0, 19.08, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', FALSE),
  (17, 17, DATE '2025-01-17', 'Gas Station', 20.21, 0.0, 20.21, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (18, 18, DATE '2025-01-18', 'Hotel', 21.34, 0.0, 21.34, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'HIGH', 'Client: auto', FALSE),
  (19, 19, DATE '2025-01-19', 'Cloud Vendor', 22.47, 0.0, 22.47, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (20, 20, DATE '2025-01-20', 'Taxi', 23.6, 0.0, 23.6, 'USD', 'OFFICE', 100, 'CASH', 'CA-US', TRUE, 'PRJ-2002', 'LOW', 'Client: auto', FALSE),
  (21, 21, DATE '2025-01-21', 'Restaurant', 24.73, 0.0, 24.73, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (22, 22, DATE '2025-01-22', 'Airline', 25.86, 0.0, 25.86, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'HIGH', 'Client: auto', FALSE),
  (23, 23, DATE '2025-01-23', 'Office Depot', 26.99, 0.0, 26.99, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (24, 24, DATE '2025-01-24', 'Gas Station', 28.12, 0.0, 28.12, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'LOW', 'Client: auto', FALSE),
  (25, 25, DATE '2025-01-25', 'Hotel', 29.25, 0.0, 29.25, 'USD', 'TRAVEL', 80, 'CASH', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (26, 26, DATE '2025-01-26', 'Cloud Vendor', 30.38, 0.0, 30.38, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (27, 27, DATE '2025-01-27', 'Taxi', 31.51, 0.0, 31.51, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (28, 28, DATE '2025-01-28', 'Restaurant', 32.64, 0.0, 32.64, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', FALSE),
  (29, 29, DATE '2025-01-29', 'Airline', 33.77, 0.0, 33.77, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (30, 30, DATE '2025-01-30', 'Office Depot', 34.9, 0.0, 34.9, 'USD', 'MEAL', 50, 'CASH', 'CA-US', TRUE, 'PRJ-1001', 'HIGH', 'Client: auto', FALSE),
  (31, 31, DATE '2025-01-31', 'Gas Station', 36.03, 0.0, 36.03, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (32, 32, DATE '2025-02-01', 'Hotel', 37.16, 0.0, 37.16, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'LOW', 'Client: auto', FALSE),
  (33, 33, DATE '2025-02-02', 'Cloud Vendor', 38.29, 0.0, 38.29, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (34, 34, DATE '2025-02-03', 'Taxi', 39.42, 0.0, 39.42, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'HIGH', 'Client: auto', FALSE),
  (35, 35, DATE '2025-02-04', 'Restaurant', 40.55, 0.0, 40.55, 'USD', 'OTHER', 100, 'CASH', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (36, 36, DATE '2025-02-05', 'Airline', 41.68, 0.0, 41.68, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'LOW', 'Client: auto', FALSE),
  (37, 37, DATE '2025-02-06', 'Office Depot', 42.81, 0.0, 42.81, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (38, 38, DATE '2025-02-07', 'Gas Station', 43.94, 0.0, 43.94, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (39, 39, DATE '2025-02-08', 'Hotel', 45.07, 0.0, 45.07, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (40, 40, DATE '2025-02-09', 'Cloud Vendor', 46.2, 0.0, 46.2, 'USD', 'SOFTWARE', 100, 'CASH', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', FALSE),
  (41, 41, DATE '2025-02-10', 'Taxi', 47.33, 0.0, 47.33, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (42, 42, DATE '2025-02-11', 'Restaurant', 48.46, 0.0, 48.46, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'HIGH', 'Client: auto', FALSE),
  (43, 43, DATE '2025-02-12', 'Airline', 49.59, 0.0, 49.59, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (44, 44, DATE '2025-02-13', 'Office Depot', 50.72, 0.0, 50.72, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'LOW', 'Client: auto', FALSE),
  (45, 45, DATE '2025-02-14', 'Gas Station', 51.85, 0.0, 51.85, 'USD', 'TRANSPORT', 100, 'CASH', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (46, 46, DATE '2025-02-15', 'Hotel', 52.98, 0.0, 52.98, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'HIGH', 'Client: auto', FALSE),
  (47, 47, DATE '2025-02-16', 'Cloud Vendor', 54.11, 0.0, 54.11, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (48, 48, DATE '2025-02-17', 'Taxi', 55.24, 0.0, 55.24, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'LOW', 'Client: auto', FALSE),
  (49, 49, DATE '2025-02-18', 'Restaurant', 56.37, 0.0, 56.37, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (50, 50, DATE '2025-02-19', 'Airline', 57.5, 0.0, 57.5, 'USD', 'OFFICE', 100, 'CASH', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (51, 51, DATE '2025-02-20', 'Office Depot', 58.63, 0.0, 58.63, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (52, 52, DATE '2025-02-21', 'Gas Station', 59.76, 0.0, 59.76, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', FALSE),
  (53, 53, DATE '2025-02-22', 'Hotel', 60.89, 0.0, 60.89, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (54, 54, DATE '2025-02-23', 'Cloud Vendor', 62.02, 0.0, 62.02, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'HIGH', 'Client: auto', FALSE),
  (55, 55, DATE '2025-02-24', 'Taxi', 63.15, 0.0, 63.15, 'USD', 'TRAVEL', 80, 'CASH', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (56, 56, DATE '2025-02-25', 'Restaurant', 64.28, 0.0, 64.28, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'LOW', 'Client: auto', FALSE),
  (57, 57, DATE '2025-02-26', 'Airline', 65.41, 0.0, 65.41, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (58, 58, DATE '2025-02-27', 'Office Depot', 66.54, 0.0, 66.54, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'HIGH', 'Client: auto', FALSE),
  (59, 59, DATE '2025-02-28', 'Gas Station', 67.67, 0.0, 67.67, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (60, 60, DATE '2025-03-01', 'Hotel', 68.8, 0.0, 68.8, 'USD', 'MEAL', 50, 'CASH', 'CA-US', TRUE, 'PRJ-1001', 'LOW', 'Client: auto', FALSE),
  (61, 61, DATE '2025-03-02', 'Cloud Vendor', 69.93, 0.0, 69.93, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (62, 62, DATE '2025-03-03', 'Taxi', 71.06, 0.0, 71.06, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (63, 63, DATE '2025-03-04', 'Restaurant', 72.19, 0.0, 72.19, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (64, 64, DATE '2025-03-05', 'Airline', 73.32, 0.0, 73.32, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', FALSE),
  (65, 65, DATE '2025-03-06', 'Office Depot', 74.45, 0.0, 74.45, 'USD', 'OTHER', 100, 'CASH', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (66, 66, DATE '2025-03-07', 'Gas Station', 75.58, 0.0, 75.58, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'HIGH', 'Client: auto', FALSE),
  (67, 67, DATE '2025-03-08', 'Hotel', 76.71, 0.0, 76.71, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (68, 68, DATE '2025-03-09', 'Cloud Vendor', 77.84, 0.0, 77.84, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'LOW', 'Client: auto', FALSE),
  (69, 69, DATE '2025-03-10', 'Taxi', 78.97, 0.0, 78.97, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (70, 70, DATE '2025-03-11', 'Restaurant', 80.1, 0.0, 80.1, 'USD', 'SOFTWARE', 100, 'CASH', 'CA-US', TRUE, NULL, 'HIGH', 'Client: auto', FALSE),
  (71, 71, DATE '2025-03-12', 'Airline', 81.23, 0.0, 81.23, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (72, 72, DATE '2025-03-13', 'Office Depot', 82.36, 0.0, 82.36, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'LOW', 'Client: auto', FALSE),
  (73, 73, DATE '2025-03-14', 'Gas Station', 83.49, 0.0, 83.49, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (74, 74, DATE '2025-03-15', 'Hotel', 84.62, 0.0, 84.62, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (75, 75, DATE '2025-03-16', 'Cloud Vendor', 85.75, 0.0, 85.75, 'USD', 'TRANSPORT', 100, 'CASH', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (76, 76, DATE '2025-03-17', 'Taxi', 86.88, 0.0, 86.88, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', FALSE),
  (77, 77, DATE '2025-03-18', 'Restaurant', 88.01, 0.0, 88.01, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (78, 78, DATE '2025-03-19', 'Airline', 89.14, 0.0, 89.14, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'HIGH', 'Client: auto', FALSE),
  (79, 79, DATE '2025-03-20', 'Office Depot', 90.27, 0.0, 90.27, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (80, 80, DATE '2025-03-21', 'Gas Station', 91.4, 0.0, 91.4, 'USD', 'OFFICE', 100, 'CASH', 'CA-US', TRUE, 'PRJ-2002', 'LOW', 'Client: auto', FALSE),
  (81, 81, DATE '2025-03-22', 'Hotel', 92.53, 0.0, 92.53, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (82, 82, DATE '2025-03-23', 'Cloud Vendor', 93.66, 0.0, 93.66, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'HIGH', 'Client: auto', FALSE),
  (83, 83, DATE '2025-03-24', 'Taxi', 94.79, 0.0, 94.79, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (84, 84, DATE '2025-03-25', 'Restaurant', 95.92, 0.0, 95.92, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'LOW', 'Client: auto', FALSE),
  (85, 85, DATE '2025-03-26', 'Airline', 97.05, 0.0, 97.05, 'USD', 'TRAVEL', 80, 'CASH', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (86, 86, DATE '2025-03-27', 'Office Depot', 98.18, 0.0, 98.18, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (87, 87, DATE '2025-03-28', 'Gas Station', 99.31, 0.0, 99.31, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (88, 88, DATE '2025-03-29', 'Hotel', 100.44, 0.0, 100.44, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', FALSE),
  (89, 89, DATE '2025-03-30', 'Cloud Vendor', 101.57, 0.0, 101.57, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (90, 90, DATE '2025-03-31', 'Taxi', 102.7, 0.0, 102.7, 'USD', 'MEAL', 50, 'CASH', 'CA-US', TRUE, 'PRJ-1001', 'HIGH', 'Client: auto', FALSE),
  (91, 91, DATE '2025-04-01', 'Restaurant', 103.83, 0.0, 103.83, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (92, 92, DATE '2025-04-02', 'Airline', 104.96, 0.0, 104.96, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'LOW', 'Client: auto', FALSE),
  (93, 93, DATE '2025-04-03', 'Office Depot', 106.09, 0.0, 106.09, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (94, 94, DATE '2025-04-04', 'Gas Station', 107.22, 0.0, 107.22, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'HIGH', 'Client: auto', FALSE),
  (95, 95, DATE '2025-04-05', 'Hotel', 108.35, 0.0, 108.35, 'USD', 'OTHER', 100, 'CASH', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (96, 96, DATE '2025-04-06', 'Cloud Vendor', 109.48, 0.0, 109.48, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'LOW', 'Client: auto', FALSE),
  (97, 97, DATE '2025-04-07', 'Taxi', 110.61, 0.0, 110.61, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (98, 98, DATE '2025-04-08', 'Restaurant', 111.74, 0.0, 111.74, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (99, 99, DATE '2025-04-09', 'Airline', 112.87, 0.0, 112.87, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (100, 100, DATE '2025-04-10', 'Office Depot', 114.0, 0.0, 114.0, 'USD', 'SOFTWARE', 100, 'CASH', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', FALSE)
) AS expected_expenses(
  expense_id, receipt_id, expense_date, vendor_name, net_amount, tax_amount, gross_amount,
  currency_code, expense_category, deductible_pct, payment_channel, jurisdiction, billable_flag,
  project_tag, documentation_quality, compliance_notes, cannot_categorize_safely
)""",
    16: """SELECT *
FROM (VALUES
  (1, 1, DATE '2025-01-01', 'Airline', 2.13, 0.0, 2.13, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (2, 2, DATE '2025-01-02', 'Office Depot', 3.26, 0.0, 3.26, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (3, 3, DATE '2025-01-03', 'Gas Station', 4.39, 0.0, 4.39, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (4, 4, DATE '2025-01-04', 'Hotel', 5.52, 0.0, 5.52, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', TRUE),
  (5, 5, DATE '2025-01-05', 'Cloud Vendor', 6.65, 0.0, 6.65, 'USD', 'OTHER', 100, 'CASH', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (6, 6, DATE '2025-01-06', 'Taxi', 7.78, 0.0, 7.78, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'HIGH', 'Client: auto', FALSE),
  (7, 7, DATE '2025-01-07', 'Restaurant', 8.91, 0.0, 8.91, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (8, 8, DATE '2025-01-08', 'Airline', 10.04, 0.0, 10.04, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'LOW', 'Client: auto', FALSE),
  (9, 9, DATE '2025-01-09', 'Office Depot', 11.17, 0.0, 11.17, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (10, 10, DATE '2025-01-10', 'Gas Station', 12.3, 0.0, 12.3, 'USD', 'SOFTWARE', 100, 'CASH', 'CA-US', TRUE, NULL, 'HIGH', 'Client: auto', TRUE),
  (11, 11, DATE '2025-01-11', 'Hotel', 13.43, 0.0, 13.43, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (12, 12, DATE '2025-01-12', 'Cloud Vendor', 14.56, 0.0, 14.56, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'LOW', 'Client: auto', FALSE),
  (13, 13, DATE '2025-01-13', 'Taxi', 15.69, 0.0, 15.69, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (14, 14, DATE '2025-01-14', 'Restaurant', 16.82, 0.0, 16.82, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (15, 15, DATE '2025-01-15', 'Airline', 17.95, 0.0, 17.95, 'USD', 'TRANSPORT', 100, 'CASH', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (16, 16, DATE '2025-01-16', 'Office Depot', 19.08, 0.0, 19.08, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', TRUE),
  (17, 17, DATE '2025-01-17', 'Gas Station', 20.21, 0.0, 20.21, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (18, 18, DATE '2025-01-18', 'Hotel', 21.34, 0.0, 21.34, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'HIGH', 'Client: auto', FALSE),
  (19, 19, DATE '2025-01-19', 'Cloud Vendor', 22.47, 0.0, 22.47, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (20, 20, DATE '2025-01-20', 'Taxi', 23.6, 0.0, 23.6, 'USD', 'OFFICE', 100, 'CASH', 'CA-US', TRUE, 'PRJ-2002', 'LOW', 'Client: auto', FALSE),
  (21, 21, DATE '2025-01-21', 'Restaurant', 24.73, 0.0, 24.73, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (22, 22, DATE '2025-01-22', 'Airline', 25.86, 0.0, 25.86, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'HIGH', 'Client: auto', TRUE),
  (23, 23, DATE '2025-01-23', 'Office Depot', 26.99, 0.0, 26.99, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (24, 24, DATE '2025-01-24', 'Gas Station', 28.12, 0.0, 28.12, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'LOW', 'Client: auto', FALSE),
  (25, 25, DATE '2025-01-25', 'Hotel', 29.25, 0.0, 29.25, 'USD', 'TRAVEL', 80, 'CASH', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (26, 26, DATE '2025-01-26', 'Cloud Vendor', 30.38, 0.0, 30.38, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (27, 27, DATE '2025-01-27', 'Taxi', 31.51, 0.0, 31.51, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (28, 28, DATE '2025-01-28', 'Restaurant', 32.64, 0.0, 32.64, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', TRUE),
  (29, 29, DATE '2025-01-29', 'Airline', 33.77, 0.0, 33.77, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (30, 30, DATE '2025-01-30', 'Office Depot', 34.9, 0.0, 34.9, 'USD', 'MEAL', 50, 'CASH', 'CA-US', TRUE, 'PRJ-1001', 'HIGH', 'Client: auto', FALSE),
  (31, 31, DATE '2025-01-31', 'Gas Station', 36.03, 0.0, 36.03, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (32, 32, DATE '2025-02-01', 'Hotel', 37.16, 0.0, 37.16, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'LOW', 'Client: auto', FALSE),
  (33, 33, DATE '2025-02-02', 'Cloud Vendor', 38.29, 0.0, 38.29, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (34, 34, DATE '2025-02-03', 'Taxi', 39.42, 0.0, 39.42, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'HIGH', 'Client: auto', TRUE),
  (35, 35, DATE '2025-02-04', 'Restaurant', 40.55, 0.0, 40.55, 'USD', 'OTHER', 100, 'CASH', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (36, 36, DATE '2025-02-05', 'Airline', 41.68, 0.0, 41.68, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'LOW', 'Client: auto', FALSE),
  (37, 37, DATE '2025-02-06', 'Office Depot', 42.81, 0.0, 42.81, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (38, 38, DATE '2025-02-07', 'Gas Station', 43.94, 0.0, 43.94, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (39, 39, DATE '2025-02-08', 'Hotel', 45.07, 0.0, 45.07, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (40, 40, DATE '2025-02-09', 'Cloud Vendor', 46.2, 0.0, 46.2, 'USD', 'SOFTWARE', 100, 'CASH', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', TRUE),
  (41, 41, DATE '2025-02-10', 'Taxi', 47.33, 0.0, 47.33, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (42, 42, DATE '2025-02-11', 'Restaurant', 48.46, 0.0, 48.46, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'HIGH', 'Client: auto', FALSE),
  (43, 43, DATE '2025-02-12', 'Airline', 49.59, 0.0, 49.59, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (44, 44, DATE '2025-02-13', 'Office Depot', 50.72, 0.0, 50.72, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'LOW', 'Client: auto', FALSE),
  (45, 45, DATE '2025-02-14', 'Gas Station', 51.85, 0.0, 51.85, 'USD', 'TRANSPORT', 100, 'CASH', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (46, 46, DATE '2025-02-15', 'Hotel', 52.98, 0.0, 52.98, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'HIGH', 'Client: auto', TRUE),
  (47, 47, DATE '2025-02-16', 'Cloud Vendor', 54.11, 0.0, 54.11, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (48, 48, DATE '2025-02-17', 'Taxi', 55.24, 0.0, 55.24, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'LOW', 'Client: auto', FALSE),
  (49, 49, DATE '2025-02-18', 'Restaurant', 56.37, 0.0, 56.37, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (50, 50, DATE '2025-02-19', 'Airline', 57.5, 0.0, 57.5, 'USD', 'OFFICE', 100, 'CASH', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (51, 51, DATE '2025-02-20', 'Office Depot', 58.63, 0.0, 58.63, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (52, 52, DATE '2025-02-21', 'Gas Station', 59.76, 0.0, 59.76, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', TRUE),
  (53, 53, DATE '2025-02-22', 'Hotel', 60.89, 0.0, 60.89, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (54, 54, DATE '2025-02-23', 'Cloud Vendor', 62.02, 0.0, 62.02, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'HIGH', 'Client: auto', FALSE),
  (55, 55, DATE '2025-02-24', 'Taxi', 63.15, 0.0, 63.15, 'USD', 'TRAVEL', 80, 'CASH', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (56, 56, DATE '2025-02-25', 'Restaurant', 64.28, 0.0, 64.28, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'LOW', 'Client: auto', FALSE),
  (57, 57, DATE '2025-02-26', 'Airline', 65.41, 0.0, 65.41, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (58, 58, DATE '2025-02-27', 'Office Depot', 66.54, 0.0, 66.54, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'HIGH', 'Client: auto', TRUE),
  (59, 59, DATE '2025-02-28', 'Gas Station', 67.67, 0.0, 67.67, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (60, 60, DATE '2025-03-01', 'Hotel', 68.8, 0.0, 68.8, 'USD', 'MEAL', 50, 'CASH', 'CA-US', TRUE, 'PRJ-1001', 'LOW', 'Client: auto', FALSE),
  (61, 61, DATE '2025-03-02', 'Cloud Vendor', 69.93, 0.0, 69.93, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (62, 62, DATE '2025-03-03', 'Taxi', 71.06, 0.0, 71.06, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (63, 63, DATE '2025-03-04', 'Restaurant', 72.19, 0.0, 72.19, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (64, 64, DATE '2025-03-05', 'Airline', 73.32, 0.0, 73.32, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', TRUE),
  (65, 65, DATE '2025-03-06', 'Office Depot', 74.45, 0.0, 74.45, 'USD', 'OTHER', 100, 'CASH', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (66, 66, DATE '2025-03-07', 'Gas Station', 75.58, 0.0, 75.58, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'HIGH', 'Client: auto', FALSE),
  (67, 67, DATE '2025-03-08', 'Hotel', 76.71, 0.0, 76.71, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (68, 68, DATE '2025-03-09', 'Cloud Vendor', 77.84, 0.0, 77.84, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'LOW', 'Client: auto', FALSE),
  (69, 69, DATE '2025-03-10', 'Taxi', 78.97, 0.0, 78.97, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (70, 70, DATE '2025-03-11', 'Restaurant', 80.1, 0.0, 80.1, 'USD', 'SOFTWARE', 100, 'CASH', 'CA-US', TRUE, NULL, 'HIGH', 'Client: auto', TRUE),
  (71, 71, DATE '2025-03-12', 'Airline', 81.23, 0.0, 81.23, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (72, 72, DATE '2025-03-13', 'Office Depot', 82.36, 0.0, 82.36, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'LOW', 'Client: auto', FALSE),
  (73, 73, DATE '2025-03-14', 'Gas Station', 83.49, 0.0, 83.49, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (74, 74, DATE '2025-03-15', 'Hotel', 84.62, 0.0, 84.62, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (75, 75, DATE '2025-03-16', 'Cloud Vendor', 85.75, 0.0, 85.75, 'USD', 'TRANSPORT', 100, 'CASH', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (76, 76, DATE '2025-03-17', 'Taxi', 86.88, 0.0, 86.88, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', TRUE),
  (77, 77, DATE '2025-03-18', 'Restaurant', 88.01, 0.0, 88.01, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (78, 78, DATE '2025-03-19', 'Airline', 89.14, 0.0, 89.14, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'HIGH', 'Client: auto', FALSE),
  (79, 79, DATE '2025-03-20', 'Office Depot', 90.27, 0.0, 90.27, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (80, 80, DATE '2025-03-21', 'Gas Station', 91.4, 0.0, 91.4, 'USD', 'OFFICE', 100, 'CASH', 'CA-US', TRUE, 'PRJ-2002', 'LOW', 'Client: auto', FALSE),
  (81, 81, DATE '2025-03-22', 'Hotel', 92.53, 0.0, 92.53, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (82, 82, DATE '2025-03-23', 'Cloud Vendor', 93.66, 0.0, 93.66, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'HIGH', 'Client: auto', TRUE),
  (83, 83, DATE '2025-03-24', 'Taxi', 94.79, 0.0, 94.79, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (84, 84, DATE '2025-03-25', 'Restaurant', 95.92, 0.0, 95.92, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'LOW', 'Client: auto', FALSE),
  (85, 85, DATE '2025-03-26', 'Airline', 97.05, 0.0, 97.05, 'USD', 'TRAVEL', 80, 'CASH', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (86, 86, DATE '2025-03-27', 'Office Depot', 98.18, 0.0, 98.18, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (87, 87, DATE '2025-03-28', 'Gas Station', 99.31, 0.0, 99.31, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (88, 88, DATE '2025-03-29', 'Hotel', 100.44, 0.0, 100.44, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', TRUE),
  (89, 89, DATE '2025-03-30', 'Cloud Vendor', 101.57, 0.0, 101.57, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (90, 90, DATE '2025-03-31', 'Taxi', 102.7, 0.0, 102.7, 'USD', 'MEAL', 50, 'CASH', 'CA-US', TRUE, 'PRJ-1001', 'HIGH', 'Client: auto', FALSE),
  (91, 91, DATE '2025-04-01', 'Restaurant', 103.83, 0.0, 103.83, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (92, 92, DATE '2025-04-02', 'Airline', 104.96, 0.0, 104.96, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'LOW', 'Client: auto', FALSE),
  (93, 93, DATE '2025-04-03', 'Office Depot', 106.09, 0.0, 106.09, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (94, 94, DATE '2025-04-04', 'Gas Station', 107.22, 0.0, 107.22, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'HIGH', 'Client: auto', TRUE),
  (95, 95, DATE '2025-04-05', 'Hotel', 108.35, 0.0, 108.35, 'USD', 'OTHER', 100, 'CASH', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (96, 96, DATE '2025-04-06', 'Cloud Vendor', 109.48, 0.0, 109.48, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'LOW', 'Client: auto', FALSE),
  (97, 97, DATE '2025-04-07', 'Taxi', 110.61, 0.0, 110.61, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (98, 98, DATE '2025-04-08', 'Restaurant', 111.74, 0.0, 111.74, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (99, 99, DATE '2025-04-09', 'Airline', 112.87, 0.0, 112.87, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (100, 100, DATE '2025-04-10', 'Office Depot', 114.0, 0.0, 114.0, 'USD', 'SOFTWARE', 100, 'CASH', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', TRUE)
) AS expected_expenses(
  expense_id, receipt_id, expense_date, vendor_name, net_amount, tax_amount, gross_amount,
  currency_code, expense_category, deductible_pct, payment_channel, jurisdiction, billable_flag,
  project_tag, documentation_quality, compliance_notes, cannot_categorize_safely
)""",
    32: """SELECT *
FROM (VALUES
  (1, 1, DATE '2025-01-01', 'Airline', 2.13, 0.0, 2.13, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (2, 2, DATE '2025-01-02', 'Office Depot', 3.26, 0.0, 3.26, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (3, 3, DATE '2025-01-03', 'Gas Station', 4.39, 0.0, 4.39, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (4, 4, DATE '2025-01-04', 'Hotel', 5.52, 0.0, 5.52, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', TRUE),
  (5, 5, DATE '2025-01-05', 'Cloud Vendor', 6.65, 0.0, 6.65, 'USD', 'OTHER', 100, 'CASH', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (6, 6, DATE '2025-01-06', 'Taxi', 7.78, 0.0, 7.78, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'HIGH', 'Client: auto', FALSE),
  (7, 7, DATE '2025-01-07', 'Restaurant', 8.91, 0.0, 8.91, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', TRUE),
  (8, 8, DATE '2025-01-08', 'Airline', 10.04, 0.0, 10.04, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'LOW', 'Client: auto', FALSE),
  (9, 9, DATE '2025-01-09', 'Office Depot', 11.17, 0.0, 11.17, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', TRUE),
  (10, 10, DATE '2025-01-10', 'Gas Station', 12.3, 0.0, 12.3, 'USD', 'SOFTWARE', 100, 'CASH', 'CA-US', TRUE, NULL, 'HIGH', 'Client: auto', TRUE),
  (11, 11, DATE '2025-01-11', 'Hotel', 13.43, 0.0, 13.43, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (12, 12, DATE '2025-01-12', 'Cloud Vendor', 14.56, 0.0, 14.56, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'LOW', 'Client: auto', FALSE),
  (13, 13, DATE '2025-01-13', 'Taxi', 15.69, 0.0, 15.69, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (14, 14, DATE '2025-01-14', 'Restaurant', 16.82, 0.0, 16.82, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (15, 15, DATE '2025-01-15', 'Airline', 17.95, 0.0, 17.95, 'USD', 'TRANSPORT', 100, 'CASH', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', TRUE),
  (16, 16, DATE '2025-01-16', 'Office Depot', 19.08, 0.0, 19.08, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', TRUE),
  (17, 17, DATE '2025-01-17', 'Gas Station', 20.21, 0.0, 20.21, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (18, 18, DATE '2025-01-18', 'Hotel', 21.34, 0.0, 21.34, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'HIGH', 'Client: auto', FALSE),
  (19, 19, DATE '2025-01-19', 'Cloud Vendor', 22.47, 0.0, 22.47, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', TRUE),
  (20, 20, DATE '2025-01-20', 'Taxi', 23.6, 0.0, 23.6, 'USD', 'OFFICE', 100, 'CASH', 'CA-US', TRUE, 'PRJ-2002', 'LOW', 'Client: auto', FALSE),
  (21, 21, DATE '2025-01-21', 'Restaurant', 24.73, 0.0, 24.73, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', TRUE),
  (22, 22, DATE '2025-01-22', 'Airline', 25.86, 0.0, 25.86, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'HIGH', 'Client: auto', TRUE),
  (23, 23, DATE '2025-01-23', 'Office Depot', 26.99, 0.0, 26.99, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (24, 24, DATE '2025-01-24', 'Gas Station', 28.12, 0.0, 28.12, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'LOW', 'Client: auto', FALSE),
  (25, 25, DATE '2025-01-25', 'Hotel', 29.25, 0.0, 29.25, 'USD', 'TRAVEL', 80, 'CASH', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (26, 26, DATE '2025-01-26', 'Cloud Vendor', 30.38, 0.0, 30.38, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (27, 27, DATE '2025-01-27', 'Taxi', 31.51, 0.0, 31.51, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (28, 28, DATE '2025-01-28', 'Restaurant', 32.64, 0.0, 32.64, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', TRUE),
  (29, 29, DATE '2025-01-29', 'Airline', 33.77, 0.0, 33.77, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (30, 30, DATE '2025-01-30', 'Office Depot', 34.9, 0.0, 34.9, 'USD', 'MEAL', 50, 'CASH', 'CA-US', TRUE, 'PRJ-1001', 'HIGH', 'Client: auto', FALSE),
  (31, 31, DATE '2025-01-31', 'Gas Station', 36.03, 0.0, 36.03, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', TRUE),
  (32, 32, DATE '2025-02-01', 'Hotel', 37.16, 0.0, 37.16, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'LOW', 'Client: auto', FALSE),
  (33, 33, DATE '2025-02-02', 'Cloud Vendor', 38.29, 0.0, 38.29, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', TRUE),
  (34, 34, DATE '2025-02-03', 'Taxi', 39.42, 0.0, 39.42, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'HIGH', 'Client: auto', TRUE),
  (35, 35, DATE '2025-02-04', 'Restaurant', 40.55, 0.0, 40.55, 'USD', 'OTHER', 100, 'CASH', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (36, 36, DATE '2025-02-05', 'Airline', 41.68, 0.0, 41.68, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'LOW', 'Client: auto', FALSE),
  (37, 37, DATE '2025-02-06', 'Office Depot', 42.81, 0.0, 42.81, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', TRUE),
  (38, 38, DATE '2025-02-07', 'Gas Station', 43.94, 0.0, 43.94, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (39, 39, DATE '2025-02-08', 'Hotel', 45.07, 0.0, 45.07, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', TRUE),
  (40, 40, DATE '2025-02-09', 'Cloud Vendor', 46.2, 0.0, 46.2, 'USD', 'SOFTWARE', 100, 'CASH', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', TRUE),
  (41, 41, DATE '2025-02-10', 'Taxi', 47.33, 0.0, 47.33, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (42, 42, DATE '2025-02-11', 'Restaurant', 48.46, 0.0, 48.46, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'HIGH', 'Client: auto', FALSE),
  (43, 43, DATE '2025-02-12', 'Airline', 49.59, 0.0, 49.59, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (44, 44, DATE '2025-02-13', 'Office Depot', 50.72, 0.0, 50.72, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'LOW', 'Client: auto', FALSE),
  (45, 45, DATE '2025-02-14', 'Gas Station', 51.85, 0.0, 51.85, 'USD', 'TRANSPORT', 100, 'CASH', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', TRUE),
  (46, 46, DATE '2025-02-15', 'Hotel', 52.98, 0.0, 52.98, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'HIGH', 'Client: auto', TRUE),
  (47, 47, DATE '2025-02-16', 'Cloud Vendor', 54.11, 0.0, 54.11, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (48, 48, DATE '2025-02-17', 'Taxi', 55.24, 0.0, 55.24, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'LOW', 'Client: auto', FALSE),
  (49, 49, DATE '2025-02-18', 'Restaurant', 56.37, 0.0, 56.37, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', TRUE),
  (50, 50, DATE '2025-02-19', 'Airline', 57.5, 0.0, 57.5, 'USD', 'OFFICE', 100, 'CASH', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (51, 51, DATE '2025-02-20', 'Office Depot', 58.63, 0.0, 58.63, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', TRUE),
  (52, 52, DATE '2025-02-21', 'Gas Station', 59.76, 0.0, 59.76, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', TRUE),
  (53, 53, DATE '2025-02-22', 'Hotel', 60.89, 0.0, 60.89, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (54, 54, DATE '2025-02-23', 'Cloud Vendor', 62.02, 0.0, 62.02, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'HIGH', 'Client: auto', FALSE),
  (55, 55, DATE '2025-02-24', 'Taxi', 63.15, 0.0, 63.15, 'USD', 'TRAVEL', 80, 'CASH', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (56, 56, DATE '2025-02-25', 'Restaurant', 64.28, 0.0, 64.28, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'LOW', 'Client: auto', FALSE),
  (57, 57, DATE '2025-02-26', 'Airline', 65.41, 0.0, 65.41, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', TRUE),
  (58, 58, DATE '2025-02-27', 'Office Depot', 66.54, 0.0, 66.54, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'HIGH', 'Client: auto', TRUE),
  (59, 59, DATE '2025-02-28', 'Gas Station', 67.67, 0.0, 67.67, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (60, 60, DATE '2025-03-01', 'Hotel', 68.8, 0.0, 68.8, 'USD', 'MEAL', 50, 'CASH', 'CA-US', TRUE, 'PRJ-1001', 'LOW', 'Client: auto', FALSE),
  (61, 61, DATE '2025-03-02', 'Cloud Vendor', 69.93, 0.0, 69.93, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', TRUE),
  (62, 62, DATE '2025-03-03', 'Taxi', 71.06, 0.0, 71.06, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (63, 63, DATE '2025-03-04', 'Restaurant', 72.19, 0.0, 72.19, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', TRUE),
  (64, 64, DATE '2025-03-05', 'Airline', 73.32, 0.0, 73.32, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', TRUE),
  (65, 65, DATE '2025-03-06', 'Office Depot', 74.45, 0.0, 74.45, 'USD', 'OTHER', 100, 'CASH', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (66, 66, DATE '2025-03-07', 'Gas Station', 75.58, 0.0, 75.58, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'HIGH', 'Client: auto', FALSE),
  (67, 67, DATE '2025-03-08', 'Hotel', 76.71, 0.0, 76.71, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (68, 68, DATE '2025-03-09', 'Cloud Vendor', 77.84, 0.0, 77.84, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'LOW', 'Client: auto', FALSE),
  (69, 69, DATE '2025-03-10', 'Taxi', 78.97, 0.0, 78.97, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (70, 70, DATE '2025-03-11', 'Restaurant', 80.1, 0.0, 80.1, 'USD', 'SOFTWARE', 100, 'CASH', 'CA-US', TRUE, NULL, 'HIGH', 'Client: auto', TRUE),
  (71, 71, DATE '2025-03-12', 'Airline', 81.23, 0.0, 81.23, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (72, 72, DATE '2025-03-13', 'Office Depot', 82.36, 0.0, 82.36, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'LOW', 'Client: auto', FALSE),
  (73, 73, DATE '2025-03-14', 'Gas Station', 83.49, 0.0, 83.49, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', TRUE),
  (74, 74, DATE '2025-03-15', 'Hotel', 84.62, 0.0, 84.62, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (75, 75, DATE '2025-03-16', 'Cloud Vendor', 85.75, 0.0, 85.75, 'USD', 'TRANSPORT', 100, 'CASH', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', TRUE),
  (76, 76, DATE '2025-03-17', 'Taxi', 86.88, 0.0, 86.88, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', TRUE),
  (77, 77, DATE '2025-03-18', 'Restaurant', 88.01, 0.0, 88.01, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (78, 78, DATE '2025-03-19', 'Airline', 89.14, 0.0, 89.14, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'HIGH', 'Client: auto', FALSE),
  (79, 79, DATE '2025-03-20', 'Office Depot', 90.27, 0.0, 90.27, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', TRUE),
  (80, 80, DATE '2025-03-21', 'Gas Station', 91.4, 0.0, 91.4, 'USD', 'OFFICE', 100, 'CASH', 'CA-US', TRUE, 'PRJ-2002', 'LOW', 'Client: auto', FALSE),
  (81, 81, DATE '2025-03-22', 'Hotel', 92.53, 0.0, 92.53, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', TRUE),
  (82, 82, DATE '2025-03-23', 'Cloud Vendor', 93.66, 0.0, 93.66, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'HIGH', 'Client: auto', TRUE),
  (83, 83, DATE '2025-03-24', 'Taxi', 94.79, 0.0, 94.79, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (84, 84, DATE '2025-03-25', 'Restaurant', 95.92, 0.0, 95.92, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'LOW', 'Client: auto', FALSE),
  (85, 85, DATE '2025-03-26', 'Airline', 97.05, 0.0, 97.05, 'USD', 'TRAVEL', 80, 'CASH', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (86, 86, DATE '2025-03-27', 'Office Depot', 98.18, 0.0, 98.18, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (87, 87, DATE '2025-03-28', 'Gas Station', 99.31, 0.0, 99.31, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', FALSE),
  (88, 88, DATE '2025-03-29', 'Hotel', 100.44, 0.0, 100.44, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', TRUE),
  (89, 89, DATE '2025-03-30', 'Cloud Vendor', 101.57, 0.0, 101.57, 'USD', 'OTHER', 100, 'CARD', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (90, 90, DATE '2025-03-31', 'Taxi', 102.7, 0.0, 102.7, 'USD', 'MEAL', 50, 'CASH', 'CA-US', TRUE, 'PRJ-1001', 'HIGH', 'Client: auto', FALSE),
  (91, 91, DATE '2025-04-01', 'Restaurant', 103.83, 0.0, 103.83, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', TRUE),
  (92, 92, DATE '2025-04-02', 'Airline', 104.96, 0.0, 104.96, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'LOW', 'Client: auto', FALSE),
  (93, 93, DATE '2025-04-03', 'Office Depot', 106.09, 0.0, 106.09, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', TRUE),
  (94, 94, DATE '2025-04-04', 'Gas Station', 107.22, 0.0, 107.22, 'USD', 'SOFTWARE', 100, 'CARD', 'CA-US', TRUE, NULL, 'HIGH', 'Client: auto', TRUE),
  (95, 95, DATE '2025-04-05', 'Hotel', 108.35, 0.0, 108.35, 'USD', 'OTHER', 100, 'CASH', 'NY-US', FALSE, 'PRJ-2002', 'HIGH', 'auto', FALSE),
  (96, 96, DATE '2025-04-06', 'Cloud Vendor', 109.48, 0.0, 109.48, 'USD', 'MEAL', 50, 'CARD', 'CA-US', TRUE, 'PRJ-1001', 'LOW', 'Client: auto', FALSE),
  (97, 97, DATE '2025-04-07', 'Taxi', 110.61, 0.0, 110.61, 'USD', 'TRAVEL', 80, 'CARD', 'NY-US', FALSE, NULL, 'HIGH', 'auto', FALSE),
  (98, 98, DATE '2025-04-08', 'Restaurant', 111.74, 0.0, 111.74, 'USD', 'OFFICE', 100, 'CARD', 'CA-US', TRUE, 'PRJ-2002', 'HIGH', 'Client: auto', FALSE),
  (99, 99, DATE '2025-04-09', 'Airline', 112.87, 0.0, 112.87, 'USD', 'TRANSPORT', 100, 'CARD', 'NY-US', FALSE, 'PRJ-1001', 'HIGH', 'auto', TRUE),
  (100, 100, DATE '2025-04-10', 'Office Depot', 114.0, 0.0, 114.0, 'USD', 'SOFTWARE', 100, 'CASH', 'CA-US', TRUE, NULL, 'LOW', 'Client: auto', TRUE)
) AS expected_expenses(
  expense_id, receipt_id, expense_date, vendor_name, net_amount, tax_amount, gross_amount,
  currency_code, expense_category, deductible_pct, payment_channel, jurisdiction, billable_flag,
  project_tag, documentation_quality, compliance_notes, cannot_categorize_safely
)""",
}

def _compare_expenses_to_expected(conn: Any, policy_count: int) -> tuple[int, int]:
    expected_values_sql = EXPECTED_VALUES_SQL_BY_POLICY_COUNT[policy_count]
    row = conn.execute(
        f"""
        WITH expected_expenses AS (
        {expected_values_sql}
        ),
        actual_expenses AS (
          SELECT
            expense_id,
            receipt_id,
            expense_date,
            vendor_name,
            net_amount,
            tax_amount,
            gross_amount,
            currency_code,
            expense_category,
            deductible_pct,
            payment_channel,
            jurisdiction,
            billable_flag,
            project_tag,
            documentation_quality,
            compliance_notes,
            cannot_categorize_safely
          FROM expenses
        ),
        mismatches AS (
          SELECT COALESCE(e.expense_id, a.expense_id) AS row_id
          FROM expected_expenses e
          FULL OUTER JOIN actual_expenses a
            ON e.expense_id = a.expense_id
          WHERE
            e.expense_id IS NULL
            OR a.expense_id IS NULL
            OR a.receipt_id IS DISTINCT FROM e.receipt_id
            OR a.expense_date IS DISTINCT FROM e.expense_date
            OR a.vendor_name IS DISTINCT FROM e.vendor_name
            OR a.net_amount IS DISTINCT FROM e.net_amount
            OR a.tax_amount IS DISTINCT FROM e.tax_amount
            OR a.gross_amount IS DISTINCT FROM e.gross_amount
            OR a.currency_code IS DISTINCT FROM e.currency_code
            OR a.expense_category IS DISTINCT FROM e.expense_category
            OR a.deductible_pct IS DISTINCT FROM e.deductible_pct
            OR a.payment_channel IS DISTINCT FROM e.payment_channel
            OR a.jurisdiction IS DISTINCT FROM e.jurisdiction
            OR a.billable_flag IS DISTINCT FROM e.billable_flag
            OR a.project_tag IS DISTINCT FROM e.project_tag
            OR a.documentation_quality IS DISTINCT FROM e.documentation_quality
            OR a.compliance_notes IS DISTINCT FROM e.compliance_notes
            OR a.cannot_categorize_safely IS DISTINCT FROM e.cannot_categorize_safely
        )
        SELECT
          (SELECT COUNT(*) FROM mismatches) AS num_differing_rows,
          (SELECT COUNT(*) FROM expenses) AS expenses_row_count
        """
    ).fetchone()
    return int(row[0]), int(row[1])


class TaxAgentStrategy(ExperimentStrategy):
    """Run tax categorization with an LLM agent under No Policy vs 1Phase handling."""

    def setup(self, context: ExperimentContext) -> None:
        self.policy_counts = [int(v) for v in context.strategy_config.get("policy_counts", DEFAULT_POLICY_COUNTS)]
        self.runs_per_setting = int(
            context.strategy_config.get("runs_per_setting", DEFAULT_RUNS_PER_SETTING)
        )
        self.include_bedrock = bool(context.strategy_config.get("include_bedrock", False))
        self.max_iterations = int(
            context.strategy_config.get("max_iterations", DEFAULT_MAX_ITERATIONS)
        )
        self.results_dir = Path(context.strategy_config.get("results_dir", "./results"))
        self.results_dir.mkdir(parents=True, exist_ok=True)

        self.claude_model = str(
            context.strategy_config.get("claude_model", DEFAULT_CLAUDE_MODEL)
        )
        self.gpt_model = str(
            context.strategy_config.get("gpt_model", DEFAULT_GPT_MODEL)
        )

        self.settings = []
        for policy_count in self.policy_counts:
            if self.include_bedrock:
                self.settings.extend(
                    [
                        ("no_policy", "bedrock", self.claude_model, policy_count),
                        ("dfc_1phase", "bedrock", self.claude_model, policy_count),
                    ]
                )
            self.settings.extend(
                [
                    ("no_policy", "openai", self.gpt_model, policy_count),
                    ("dfc_1phase", "openai", self.gpt_model, policy_count),
                ]
            )

    def _setting_and_run(self, execution_number: int) -> tuple[str, str, str, int, int]:
        setting_index = (execution_number - 1) // self.runs_per_setting
        run_num = ((execution_number - 1) % self.runs_per_setting) + 1
        approach, provider, model_name, policy_count = self.settings[setting_index]
        return approach, provider, model_name, policy_count, run_num

    def execute(self, context: ExperimentContext) -> ExperimentResult:
        from agent_harness.agent import build_agent, run_agent_loop
        from agent_harness.config import HarnessConfig

        approach, provider, model_name, policy_count, run_num = self._setting_and_run(
            context.execution_number
        )
        phase = "warmup" if context.is_warmup else f"run {run_num}"
        print(
            f"[Execution {context.execution_number}] tax_agent "
            f"approach={approach} provider={provider} model={model_name} "
            f"policies={policy_count} ({phase})"
        )

        db_path = self.results_dir / f"tax_agent_exec_{context.execution_number}.duckdb"
        if db_path.exists():
            db_path.unlink()
        _create_tax_tables(str(db_path))

        base_config = HarnessConfig.from_env()
        system_prompt = (
            "You are a SQL tax agent. Use execute_sql for all work. "
            "If execute_sql returns policy_feedback, treat it as a hard failure, "
            "fix the SQL, and retry until violations are resolved. "
            "All writes to expenses must use INSERT INTO ... SELECT ... FROM receipts. "
            "Example: INSERT INTO expenses (expense_id, receipt_id, expense_date, vendor_name, net_amount, "
            "tax_amount, gross_amount, currency_code, expense_category, deductible_pct, payment_channel, "
            "jurisdiction, billable_flag, project_tag, documentation_quality, compliance_notes, "
            "cannot_categorize_safely) "
            "SELECT receipt_id, receipt_id, tx_date, merchant, amount, 0, amount, currency, category, 100, "
            "payment_method, state || '-US', client_billable, project_code, 'HIGH', 'auto', FALSE "
            "FROM receipts;"
        )
        config = replace(
            base_config,
            provider=provider,
            db_path=str(db_path),
            openai_model=model_name if provider == "openai" else base_config.openai_model,
            bedrock_model_id=model_name if provider == "bedrock" else base_config.bedrock_model_id,
            max_result_rows=1000,
            verbose=False,
            system_prompt=system_prompt,
        )
        config.validate()

        agent, sql_harness = build_agent(config)

        try:
            policies = _build_policies(policy_count)
            for policy in policies:
                sql_harness.register_policy(policy)

            sql_harness.set_policy_mode(
                mode="enforce" if approach == "dfc_1phase" else "observe",
                invalid_table_names=["expenses"],
            )
            # Ensure each run starts from an empty expenses table.
            sql_harness.conn.execute("DELETE FROM expenses")

            policy_descriptions = [policy.description or policy.constraint for policy in policies]
            prompt = _tax_agent_prompt(policy_descriptions)
            chat_history: list[Any] = []

            total_start = time.perf_counter()
            final_output, loop_stats = run_agent_loop(
                agent=agent,
                user_input=prompt,
                chat_history=chat_history,
                max_iterations=self.max_iterations,
                return_stats=True,
            )
            total_time = (time.perf_counter() - total_start) * 1000.0

            tool_payloads = _extract_tool_payloads(chat_history)
            rewrite_time_ms = sum(float(p.get("rewrite_time_ms", 0.0)) for p in tool_payloads)
            exec_time_ms = sum(float(p.get("exec_time_ms", 0.0)) for p in tool_payloads)
            policy_violation_count = sum(int(p.get("policy_violation_count", 0)) for p in tool_payloads)
            tool_calls = len(tool_payloads)
            failure_counts_by_policy = _policy_failure_counts(tool_payloads, policy_descriptions)

            num_differing_rows, expenses_row_count = _compare_expenses_to_expected(
                sql_harness.conn,
                policy_count,
            )
            invalid_rows = sql_harness.conn.execute(
                "SELECT COUNT(*) FROM expenses WHERE COALESCE(invalid_string, '') <> ''"
            ).fetchone()[0]
            cannot_categorize_safely_rows = sql_harness.conn.execute(
                "SELECT COUNT(*) FROM expenses WHERE cannot_categorize_safely = TRUE"
            ).fetchone()[0]

            custom_metrics = {
                "query_type": "TAX_AGENT",
                "approach": approach,
                "provider": provider,
                "model_name": model_name,
                "policy_count": policy_count,
                "run_num": run_num if not context.is_warmup else 0,
                "agent_time_ms": total_time,
                "dfc_1phase_rewrite_time_ms": rewrite_time_ms,
                "dfc_1phase_exec_time_ms": exec_time_ms,
                "tool_calls": tool_calls,
                "agent_turns": loop_stats.llm_turns,
                "llm_input_chars": loop_stats.chars_sent_to_llm,
                "policy_violation_count": policy_violation_count,
                "num_differing_rows": num_differing_rows,
                "expenses_row_count": expenses_row_count,
                "expenses_rows": expenses_row_count,
                "expenses_invalid_rows": invalid_rows,
                "expenses_cannot_categorize_safely_rows": cannot_categorize_safely_rows,
                "policy_failure_counts_json": json.dumps(failure_counts_by_policy, sort_keys=True),
                "agent_error": "",
                "agent_output": final_output[:1000],
            }
            for idx, description in enumerate(policy_descriptions, start=1):
                custom_metrics[f"policy_{idx}_failed_rows"] = failure_counts_by_policy.get(
                    description, 0
                )
            return ExperimentResult(duration_ms=total_time, custom_metrics=custom_metrics)
        except Exception as exc:
            return ExperimentResult(
                duration_ms=0.0,
                error=str(exc),
                custom_metrics={
                    "query_type": "TAX_AGENT",
                    "approach": approach,
                    "provider": provider,
                    "model_name": model_name,
                    "policy_count": policy_count,
                    "run_num": run_num if not context.is_warmup else 0,
                    "agent_error": str(exc),
                },
            )
        finally:
            with contextlib.suppress(Exception):
                sql_harness.close()

    def teardown(self, _context: ExperimentContext) -> None:
        return None

    def get_metrics(self) -> list[str]:
        return [
            "query_type",
            "approach",
            "provider",
            "model_name",
            "policy_count",
            "run_num",
            "agent_time_ms",
            "dfc_1phase_rewrite_time_ms",
            "dfc_1phase_exec_time_ms",
            "tool_calls",
            "agent_turns",
            "llm_input_chars",
            "policy_violation_count",
            "num_differing_rows",
            "expenses_row_count",
            "expenses_rows",
            "expenses_invalid_rows",
            "expenses_cannot_categorize_safely_rows",
            "policy_failure_counts_json",
            "agent_error",
            "agent_output",
        ]

    def get_setting_key(self, context: ExperimentContext) -> Any | None:
        approach, provider, model_name, policy_count, _ = self._setting_and_run(
            context.execution_number
        )
        return (approach, provider, model_name, policy_count)
