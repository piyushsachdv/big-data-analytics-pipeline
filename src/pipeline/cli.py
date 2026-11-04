from __future__ import annotations

import argparse
import json

from src.pipeline.orchestrator import run_pipeline


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the e-commerce pipeline on YARN.")
    parser.add_argument("csv_path", help="Path to the input CSV file")
    args = parser.parse_args()

    def report(event: dict) -> None:
        print(json.dumps(event, default=str), flush=True)

    result = run_pipeline(args.csv_path, callback=report)
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
