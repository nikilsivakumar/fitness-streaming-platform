"""
Phase 1 end-to-end test.
Runs simulator for 30 seconds, writes Bronze, verifies output.
"""

import os
import sys
import threading
import time

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from version_local.producer.wearable_simulator import run_simulator
from version_local.consumer.bronze_writer import run_bronze_writer

BASE_DATA_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'bronze')


def verify_bronze_output():
    """Checks Bronze folder has expected structure and files."""
    print("\n" + "=" * 60)
    print("PHASE 1 VERIFICATION")
    print("=" * 60)

    expected_types = [
        "wearable_event", "workout_log", "sleep_log",
        "nutrition_snapshot", "user_profile"
    ]

    all_passed = True

    for record_type in expected_types:
        pattern = f"record_type={record_type}"
        found = False

        for root, dirs, files in os.walk(BASE_DATA_PATH):
            if pattern in root and any(f.endswith('.parquet') for f in files):
                parquet_files = [f for f in files if f.endswith('.parquet')]
                print(f"  ✓ {record_type}: {len(parquet_files)} parquet file(s) found")
                found = True
                break

        if not found:
            print(f"  ✗ {record_type}: NO FILES FOUND")
            all_passed = False

    print("\n" + ("✅ Phase 1 PASSED" if all_passed else "❌ Phase 1 FAILED"))
    return all_passed


def main():
    print("Starting Phase 1 end-to-end test (30 seconds)...")
    print("Simulator and Bronze writer running in parallel threads.\n")

    # Run simulator in background thread
    sim_thread = threading.Thread(
        target=run_simulator,
        kwargs={"records_per_batch": 10, "sleep_seconds": 1.0,
                "max_batches": 20, "silent": False}
    )

    # Run bronze writer in background thread
    writer_thread = threading.Thread(
        target=run_bronze_writer,
        kwargs={"poll_interval": 2.0, "max_cycles": 15, "silent": False}
    )

    sim_thread.start()
    writer_thread.start()

    sim_thread.join()
    writer_thread.join()

    verify_bronze_output()


if __name__ == "__main__":
    main()