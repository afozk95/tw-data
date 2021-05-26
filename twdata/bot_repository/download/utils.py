from typing import Optional, Union
from pathlib import Path
import requests
import os
from ..utils import (
    DATASETS,
    make_dataset_filename,
)


def make_data_request(url: str) -> requests.Request:
    r = requests.get(url, stream=True)
    return r


def make_raw_dataset_path(dataset_name: str) -> Path:
    filename = make_dataset_filename(DATASETS[dataset_name]["url"], DATASETS[dataset_name].get("filename", None))
    path = Path(__file__).parents[1].resolve() / "data" / "bot_repository" / "raw" / filename
    return path


def write_data_to_disk(path: Union[str, Path], data: bytes) -> None:
    parent_path = Path(path).parents[0].resolve()
    os.makedirs(parent_path, exist_ok=True)
    with open(path, "wb+") as f:
        f.write(data)
