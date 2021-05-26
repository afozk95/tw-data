
from typing import Any, Dict, List, Optional
import json
import pandas as pd
from pathlib import Path


def read_json(filepath: str) -> Any:
    with open(filepath) as f:
        data = json.load(f)

    return data


def get_dataset_path(
    datasets_folder_path: str,
    dataset_name: str,
) -> Path:
    return Path(datasets_folder_path) / dataset_name


def read_dataset_helper(datasets_folder_path: str, dataset_name: str) -> pd.DataFrame:
    DATASET_LABELS_READ_FUNCTION = {
        "verified-2019": read_generic_dataset_df,
        "botwiki-2019": read_generic_dataset_df,
        "cresci-rtbust-2019": read_generic_dataset_df,
        "political-bots-2019": read_generic_dataset_df,
        "botometer-feedback-2019": read_generic_dataset_df,
        "vendor-purchased-2019": read_generic_dataset_df,
        "celebrity-2019": read_generic_dataset_df,
        "pronbots-2019": read_generic_dataset_df,
        "cresci-stock-2018": read_generic_dataset_df,
        "gilani-2017": read_generic_dataset_df,
        "midterm-2018": read_midterm_2018_df,
        "varol-2017": read_varol_2017_user_labels_df,
        "caverlee-2011": read_caverlee_2011_user_labels_df,
        "cresci-2017": read_cresci_2017_user_labels_df,
        "cresci-2015": read_cresci_2015_user_labels_df,
        "kaiser": read_generic_user_labels_df,
        "astroturf": read_generic_user_labels_df,
    }

    dataset_path = get_dataset_path(datasets_folder_path, dataset_name)
    dataset_read_func = DATASET_LABELS_READ_FUNCTION[dataset_name]
    df = dataset_read_func(dataset_path)
    df["dataset_name"] = dataset_name
    df = make_custom_label(df, dataset_name)
    df = make_top_level_label(df, dataset_name)
    return df


def read_generic_dataset_df(path: Path) -> pd.DataFrame:
    df_labels = read_generic_user_labels_df(path)
    df_crawls = read_generic_user_crawls_df(path)
    df = df_labels.join(df_crawls.set_index("user_id"), how="inner", on="user_id")
    return df


def read_generic_user_labels_df(path: Path) -> pd.DataFrame:
    user_labels_path = path / f"{path.name}.tsv"
    df = pd.read_csv(user_labels_path, delimiter="\t", names=["user_id", "original_label"])
    df["user_id"] = df["user_id"].astype(str)
    return df


def read_generic_user_crawls_df(path: Path) -> pd.DataFrame:
    user_crawls_path = path / f"{path.name}_tweets.json"
    user_crawls = read_json(user_crawls_path)
    rows = map(lambda dct: (dct["user"]["id_str"], dct), user_crawls)
    df = pd.DataFrame(rows, columns=["user_id", "user_crawl_object"])
    df["user_id"] = df["user_id"].astype(str)
    return df


def read_midterm_2018_df(path: Path) -> pd.DataFrame:
    df_labels = read_generic_user_labels_df(path)
    df_crawls = read_midterm_2018_user_crawls_df(path)
    df = df_labels.join(df_crawls.set_index("user_id"), how="outer", on="user_id")
    return df


def read_midterm_2018_user_crawls_df(path: Path) -> pd.DataFrame:
    def process_element(dct: Dict[str, Any]) -> Dict[str, Any]:
        created_at = dct["probe_timestamp"]
        user_obj = dct.copy()
        user_obj["id"] = user_obj["user_id"]
        user_obj["id_str"] = str(user_obj["user_id"])
        user_obj["created_at"] = user_obj["user_created_at"]
        del user_obj["user_id"]
        del user_obj["user_created_at"]
        del user_obj["probe_timestamp"]
        return {
            "created_at": created_at,
            "user": user_obj,
        }

    user_crawls_path = path / f"{path.name}_processed_user_objects.json"
    user_crawls = map(process_element, read_json(user_crawls_path))
    rows = map(lambda dct: (dct["user"]["id_str"], dct), user_crawls)
    df = pd.DataFrame(rows, columns=["user_id", "user_crawl_object"])
    df["user_id"] = df["user_id"].astype(str)
    return df


def read_varol_2017_user_labels_df(path: Path) -> pd.DataFrame:
    user_labels_path = path / f"{path.name}.dat"
    df = pd.read_table(user_labels_path, sep=r"\s+", names=["user_id", "original_label"])
    df["user_id"] = df["user_id"].astype(str)
    df["original_label"] = df["original_label"].astype(str)
    return df


def read_caverlee_2011_user_labels_df(path: Path) -> pd.DataFrame:
    inner_path = path / "social_honeypot_icwsm_2011"
    content_polluters_path = inner_path / "content_polluters.txt"
    legitimate_users_path = inner_path / "legitimate_users.txt"
    column_names = [
        "user_id",
        "created_at",
        "collected_at",
        "number_of_followings",
        "number_of_followers",
        "number_of_tweets",
        "length_of_screen_name",
        "length_of_description_in_user_profile",
    ]
    df_content_polluters = pd.read_csv(content_polluters_path, delimiter="\t", names=column_names)
    df_content_polluters["original_label"] = "content_polluter"
    df_legitimate_users = pd.read_csv(legitimate_users_path, delimiter="\t", names=column_names)
    df_legitimate_users["original_label"] = "legitimate_user"
    df = pd.concat([df_content_polluters, df_legitimate_users])
    df["user_id"] = df["user_id"].astype(str)
    return df


def read_cresci_2015_user_labels_df(path: Path) -> pd.DataFrame:
    dfs = []
    for csv_path in path.glob("**/users.csv"):
        df_tmp = pd.read_csv(csv_path)
        df_tmp.rename(columns={"id": "user_id"}, inplace=True)
        df_tmp["original_label"] = df_tmp["dataset"]
        dfs.append(df_tmp)

    df = pd.concat(dfs)
    df["user_id"] = df["user_id"].astype(str)
    return df


def read_cresci_2017_user_labels_df(path: Path) -> pd.DataFrame:
    dfs = []
    for csv_path in path.glob("**/users.csv"):
        df_tmp = pd.read_csv(csv_path)
        df_tmp.rename(columns={"id": "user_id"}, inplace=True)
        df_tmp["original_label"] = csv_path.parent.name.replace(".csv", "")
        dfs.append(df_tmp)

    df = pd.concat(dfs)
    df["user_id"] = df["user_id"].astype(str)
    return df


def make_custom_label(
    df: pd.DataFrame,
    dataset_name: str,
    original_label_col_name: str = "original_label",
    custom_label_col_name: str = "custom_label",
) -> pd.DataFrame:
    ORIGINAL_LABEL_TO_CUSTOM_LABEL_BY_DATASET = {
        "cresci-2017": {
            "traditional_spambots_1": "spammer",
            "traditional_spambots_2": "spammer",
            "traditional_spambots_3": "spammer",
            "traditional_spambots_4": "spammer",
            "social_spambots_1": "spammer",
            "social_spambots_2": "spammer",
            "social_spambots_3": "spammer",
            "fake_followers": "fake_follower",
            "genuine_accounts": "human",
        },
        "cresci-2015": {
            "TFP": "human",
            "E13": "human",
            "FSF": "other",
            "INT": "other",
            "TWT": "other",
        },
        "caverlee-2011": {
            "legitimate_user": "human",
            "content_polluter": "simple",
        },
        "varol-2017": {
            "0": "human",
            "1": "other",
        },
        "midterm-2018": {
            "human": "human",
            "bot": "other",
        },
        "gilani-2017": {
            "human": "human",
            "bot": "other",
        },
        "cresci-stock-2018": {
            "human": "human",
            "bot": "financial",
        },
        "pronbots-2019": {
            "bot": "spammer",
        },
        "verified-2019": {
            "human": "human",
        },
        "botwiki-2019": {
            "bot": "self_declared",
        },
        "cresci-rtbust-2019": {
            "human": "human",
            "bot": "other",
        },
        "political-bots-2019": {
            "bot": "political",
        },
        "botometer-feedback-2019": {
            "human": "human",
            "bot": "other",
        },
        "vendor-purchased-2019": {
            "bot": "fake_follower",
        },
        "celebrity-2019": {
            "human": "human",
        },
        "kaiser": {
            "German_Bot": "other",
            "US_Politician": "human",
            "German_Politician": "human",
        },
        "astroturf": {
            "political_Bot": "political",
        },
    }

    original_label_to_custom_label = ORIGINAL_LABEL_TO_CUSTOM_LABEL_BY_DATASET[dataset_name]
    df[custom_label_col_name] = df[original_label_col_name].apply(lambda x: original_label_to_custom_label[x])

    return df


def make_top_level_label(
    df: pd.DataFrame,
    dataset_name: str,
    original_label_col_name: str = "original_label",
    custom_label_col_name: str = "custom_label",
    top_level_label_col_name: str = "top_level_label",
) -> pd.DataFrame:
    ORIGINAL_LABEL_TO_TOP_LEVEL_LABEL_BY_DATASET = {
        "cresci-2017": {
            "traditional_spambots_1": "bot",
            "traditional_spambots_2": "bot",
            "traditional_spambots_3": "bot",
            "traditional_spambots_4": "bot",
            "social_spambots_1": "bot",
            "social_spambots_2": "bot",
            "social_spambots_3": "bot",
            "fake_followers": "bot",
            "genuine_accounts": "human",
        },
        "cresci-2015": {
            "TFP": "human",
            "E13": "human",
            "FSF": "bot",
            "INT": "bot",
            "TWT": "bot",
        },
        "caverlee-2011": {
            "legitimate_user": "human",
            "content_polluter": "bot",
        },
        "varol-2017": {
            "0": "human",
            "1": "bot",
        },
        "midterm-2018": {
            "human": "human",
            "bot": "bot",
        },
        "gilani-2017": {
            "human": "human",
            "bot": "bot",
        },
        "cresci-stock-2018": {
            "human": "human",
            "bot": "bot",
        },
        "pronbots-2019": {
            "bot": "bot",
        },
        "verified-2019": {
            "human": "human",
        },
        "botwiki-2019": {
            "bot": "bot",
        },
        "cresci-rtbust-2019": {
            "human": "human",
            "bot": "bot",
        },
        "political-bots-2019": {
            "bot": "bot",
        },
        "botometer-feedback-2019": {
            "human": "human",
            "bot": "bot",
        },
        "vendor-purchased-2019": {
            "bot": "bot",
        },
        "celebrity-2019": {
            "human": "human",
        },
        "kaiser": {
            "German_Bot": "bot",
            "US_Politician": "human",
            "German_Politician": "human",
        },
        "astroturf": {
            "political_Bot": "bot",
        },
    }

    original_label_to_top_level_label = ORIGINAL_LABEL_TO_TOP_LEVEL_LABEL_BY_DATASET[dataset_name]
    df[top_level_label_col_name] = df[original_label_col_name].apply(lambda x: original_label_to_top_level_label[x])

    return df


def combine_datasets_helper(
    datasets: Dict[str, pd.DataFrame],
    is_user_crawl_object_exists: Optional[bool] = None,
) -> pd.DataFrame:
    df = pd.concat(list(datasets.values()))
    df = df[["user_id", "top_level_label", "custom_label", "original_label", "dataset_name", "user_crawl_object"]]

    if is_user_crawl_object_exists is True:
        df = df[df["user_crawl_object"].notna()]
    elif is_user_crawl_object_exists is False:
        df = df[df["user_crawl_object"].isna()]

    df.reset_index(drop=True, inplace=True)
    return df
