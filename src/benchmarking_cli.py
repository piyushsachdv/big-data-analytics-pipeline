"""Command-line entry point for one explicitly selected YARN benchmark."""

from __future__ import annotations

import argparse
import json

from src.benchmarking import run_benchmark
from utils.data_generator import ROWS


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate and run one synthetic YARN benchmark.")
    parser.add_argument("rows", type=int, choices=ROWS)
    parser.add_argument("--repetition", type=int, default=1)
    args = parser.parse_args()
    print(json.dumps(run_benchmark(args.rows, repetition=args.repetition), indent=2), flush=True)


if __name__ == "__main__":
    main()
