from typing import Optional
from pathlib import Path


DATASETS = {
    "astroturf": {
        "url": "https://botometer.osome.iu.edu/bot-repository/datasets/astroturf/astroturf.tar.gz",
        "filename": "astroturf.tar.gz",
    },
    "kaiser": {
        "url": "https://botometer.osome.iu.edu/bot-repository/datasets/kaiser/kaiser.tar.gz",
        "filename": "kaiser.tar.gz",
    },
    "verified-2019": {
        "url": "https://botometer.osome.iu.edu/bot-repository/datasets/verified-2019/verified-2019.tar.gz",
        "filename": "verified-2019.tar.gz",
    },
    "botwiki-2019": {
        "url": "https://botometer.osome.iu.edu/bot-repository/datasets/botwiki-2019/botwiki-2019.tar.gz",
        "filename": "botwiki-2019.tar.gz",
    },
    "cresci-rtbust-2019": {
        "url": "https://botometer.osome.iu.edu/bot-repository/datasets/cresci-rtbust-2019/cresci-rtbust-2019.tar.gz",
        "filename": "cresci-rtbust-2019.tar.gz",
    },
    "political-bots-2019": {
        "url": "https://botometer.osome.iu.edu/bot-repository/datasets/political-bots-2019/political-bots-2019.tar.gz",
        "filename": "political-bots-2019.tar.gz",
    },
    "botometer-feedback-2019": {
        "url": "https://botometer.osome.iu.edu/bot-repository/datasets/botometer-feedback-2019/botometer-feedback-2019.tar.gz",
        "filename": "botometer-feedback-2019.tar.gz",
    },
    "vendor-purchased-2019": {
        "url": "https://botometer.osome.iu.edu/bot-repository/datasets/vendor-purchased-2019/vendor-purchased-2019.tar.gz",
        "filename": "vendor-purchased-2019.tar.gz",
    },
    "celebrity-2019": {
        "url": "https://botometer.osome.iu.edu/bot-repository/datasets/celebrity-2019/celebrity-2019.tar.gz",
        "filename": "celebrity-2019.tar.gz",
    },
    "pronbots-2019": {
        "url": "https://botometer.osome.iu.edu/bot-repository/datasets/pronbots-2019/pronbots-2019.tar.gz",
        "filename": "pronbots-2019.tar.gz",
    },
    "midterm-2018": {
        "url": "https://botometer.osome.iu.edu/bot-repository/datasets/midterm-2018/midterm-2018.tar.gz",
        "filename": "midterm-2018.tar.gz",
    },
    "cresci-stock-2018": {
        "url": "https://botometer.osome.iu.edu/bot-repository/datasets/cresci-stock-2018/cresci-stock-2018.tar.gz",
        "filename": "cresci-stock-2018.tar.gz",
    },
    "gilani-2017": {
        "url": "https://botometer.osome.iu.edu/bot-repository/datasets/gilani-2017/gilani-2017.tar.gz",
        "filename": "gilani-2017.tar.gz",
    },
    "varol-2017": {
        "url": "https://botometer.osome.iu.edu/bot-repository/datasets/varol-2017/varol-2017.dat.gz",
        "filename": "varol-2017.dat.gz",
    },
    "caverlee-2011": {
        "url": "https://botometer.osome.iu.edu/bot-repository/datasets/caverlee-2011/caverlee-2011.zip",
        "filename": "caverlee-2011.zip",
    },
    "cresci-2017": {
        "url": "https://botometer.osome.iu.edu/bot-repository/datasets/cresci-2017/cresci-2017.csv.zip",
        "filename": "cresci-2017.csv.zip",
    },
    "cresci-2015": {
        "url": "https://botometer.osome.iu.edu/bot-repository/datasets/cresci-2015/cresci-2015.csv.tar.gz",
        "filename": "cresci-2015.csv.tar.gz",
    },
}


def make_dataset_filename(url: str, filename: Optional[str] = None) -> str:
    if not isinstance(filename, str):
        filename = Path(url).name

    return filename
