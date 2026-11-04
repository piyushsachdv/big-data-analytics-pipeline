# Dataset Profile and Test Guidance

## Provenance and reproducibility
The project describes the included transaction files as synthetic, fictional educational data. `utils/data_generator.py` is deterministic for a given seed and creates new fixtures with deliberately injected data-quality defects.

The committed 50,000-row demonstration CSV does not byte-match the generator's default output. Its specific source seed or generation procedure is not recorded in the repository, so the file is checksum-verifiable but not currently reproducible from the documented default generator. Confirm and record its provenance before public release; do not represent the default seed as recreating that exact file.

## Recommended use
1. Use `ecommerce_edge_cases.csv` for unit tests of individual cleaning rules.
2. Use `ecommerce_transactions_test_1000.csv` for fast end-to-end development tests.
3. Use `ecommerce_transactions_clean_1000.csv` as a clean control dataset.
4. Use `ecommerce_transactions_50000.csv` for the first full demonstration.
5. Generate larger 100k/250k/500k files through the application's own deterministic data generator for performance experiments.

## Important
Do not treat the rows as real business observations. The project represents this data as synthetic and suitable for software testing and demonstration only. The transaction schema includes customer names and IDs; retain that synthetic-data provenance in any redistribution. Do not claim the benchmark results are representative of a production cluster.
