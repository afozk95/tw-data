from typing import List, Optional, Union
from .utils import (
    extract_archive,
    process_dataset_extra_steps,
    make_processed_dataset_path,
)
from ..download.utils import make_raw_dataset_path
from ..utils import DATASETS


def process_datasets(dataset_names: Optional[Union[str, List[str]]] = None, is_verbose: bool = True) -> None:
    if dataset_names is None:
        dataset_names = list(DATASETS.keys())
    elif isinstance(dataset_names, str):
        dataset_names = [dataset_names]
    elif isinstance(dataset_names, list):
        pass
    else:
        raise ValueError("Unknown type for dataset_names")

    if is_verbose and len(dataset_names) > 0:
        print("Processing datasets..")

    for i, dataset_name in enumerate(dataset_names, start=1):
        if is_verbose:
            print(f"{i}/{len(dataset_names)}: {dataset_name}")
        process_dataset(dataset_name)


def process_dataset(dataset_name: str) -> None:
    path = make_raw_dataset_path(dataset_name)
    extract_path = make_processed_dataset_path(dataset_name)
    extract_archive(path, extract_path)
    process_dataset_extra_steps(dataset_name, extract_path)
