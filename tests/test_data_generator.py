import pandas as pd

from utils.data_generator import generate_dataset


def test_generator_is_deterministic_for_same_seed(tmp_path):
    first = generate_dataset(10_000, tmp_path / "one.csv", seed=77, inject_defects=True)
    second = generate_dataset(10_000, tmp_path / "two.csv", seed=77, inject_defects=True)
    assert pd.read_csv(first).equals(pd.read_csv(second))
