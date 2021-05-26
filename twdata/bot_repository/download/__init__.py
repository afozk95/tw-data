from typing import List, Optional, Union
from pathlib import Path
from .utils import (
    make_data_request,
    make_raw_dataset_path,
    write_data_to_disk,
)
from ..utils import DATASETS


def download_datasets(dataset_names: Optional[Union[str, List[str]]] = None, is_verbose: bool = True) -> None:
    if dataset_names is None:
        dataset_names = list(DATASETS.keys())
    elif isinstance(dataset_names, str):
        dataset_names = [dataset_names]
    elif isinstance(dataset_names, list):
        pass
    else:
        raise ValueError("Unknown type for dataset_names")

    if is_verbose and len(dataset_names) > 0:
        print("Downloading datasets..")

    for i, dataset_name in enumerate(dataset_names, start=1):
        if is_verbose:
            print(f"{i}/{len(dataset_names)}: {dataset_name}")
        download_dataset(dataset_name)


def download_dataset(dataset_name: str) -> None:
    if dataset_name in DATASETS.keys():
        url = DATASETS[dataset_name].get("url", None)

        if url is None:
            raise ValueError(f"No url for dataset_name = {dataset_name}")

        r = make_data_request(url)

        if r.ok:
            path = make_raw_dataset_path(dataset_name)
            write_data_to_disk(path, r.content)
        else:
            raise ValueError("Error occurred in request")

    else:
        raise ValueError(f"dataset_name = {dataset_name} is not recognized")
