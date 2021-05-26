from typing import Dict, List, Optional, Union
import pandas as pd
from pathlib import Path
from .utils import (
    read_dataset_helper,
    combine_datasets_helper,
)
from ..utils import DATASETS


def combine_datasets(
    dataset_names: Optional[Union[str, List[str]]] = None,
    is_user_crawl_object_exists: Optional[bool] = None,
    is_verbose: bool = False,
) -> pd.DataFrame:
    datasets = read_datasets(dataset_names, is_verbose)
    df = combine_datasets_helper(datasets, is_user_crawl_object_exists)
    return df


def read_datasets(dataset_names: Optional[Union[str, List[str]]] = None, is_verbose: bool = True) -> None:
    if dataset_names is None:
        dataset_names = list(DATASETS.keys())
    elif isinstance(dataset_names, str):
        dataset_names = [dataset_names]
    elif isinstance(dataset_names, list):
        pass
    else:
        raise ValueError("Unknown type for dataset_names")

    if is_verbose and len(dataset_names) > 0:
        print("Reading datasets..")

    dfs = []
    for i, dataset_name in enumerate(dataset_names, start=1):
        if is_verbose:
            print(f"{i}/{len(dataset_names)}: {dataset_name}")
        df = read_dataset(dataset_name)
        dfs.append(df)

    return dict(zip(dataset_names, dfs))


def read_dataset(dataset_name: str) -> pd.DataFrame:
    datasets_folder_path = Path(__file__).parents[1].resolve() / "data" / "bot_repository" / "processed"
    return read_dataset_helper(datasets_folder_path, dataset_name)
