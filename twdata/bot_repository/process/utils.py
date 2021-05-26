from typing import Optional, Union
from pathlib import Path
import gzip
import os
import re
import shutil
from ..utils import (
    DATASETS,
    make_dataset_filename,
)


def make_processed_dataset_path(dataset_name: str) -> Path:
    filename = make_dataset_filename(DATASETS[dataset_name]["url"], DATASETS[dataset_name].get("filename", None))
    basename = os.path.basename(filename)
    basestem = basename[:basename.index(".")] if "." in basename else basename
    path = Path(__file__).parents[1].resolve() / "data" / "bot_repository" / "processed" / basestem
    return path


def extract_gz(path: Union[str, Path], extract_path: Optional[Union[str, Path]] = None) -> None:
    if extract_path is None:
        extract_path = os.curdir

    filename = os.path.split(path)[-1]
    filename = re.sub(r"\.gz$", "", filename, flags=re.IGNORECASE)

    os.makedirs(extract_path, exist_ok=True)

    with gzip.open(path, "rb") as f_in:
        with open(os.path.join(extract_path, filename), "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)


def extract_archive(path: Union[str, Path], extract_path: Optional[Union[str, Path]] = None) -> None:
    if "gz" not in [el[0] for el in shutil.get_unpack_formats()]:
        shutil.register_unpack_format("gz", [".gz"], extract_gz, description="gzip'ed file")
    shutil.unpack_archive(path, extract_path)


def process_dataset_extra_steps(dataset_name: str, extract_path: Union[str, Path]) -> None:
    if dataset_name == "cresci-2015":
        process_cresci_2015_dataset(extract_path)
    elif dataset_name == "cresci-2017":
        process_cresci_2017_dataset(extract_path)


def process_cresci_2015_dataset(extract_path: Union[str, Path]) -> None:
    for zip_file_path in extract_path.glob("*.zip"):
        zip_file_extract_path = Path(zip_file_path).parent / Path(zip_file_path).stem
        extract_archive(zip_file_path, zip_file_extract_path)
        os.remove(zip_file_path)


def process_cresci_2017_dataset(extract_path: Union[str, Path]) -> None:
    extract_path = Path(extract_path) / "datasets_full.csv"
    for zip_file_path in extract_path.glob("*.zip"):
        extract_archive(zip_file_path, extract_path)
        os.remove(zip_file_path)
