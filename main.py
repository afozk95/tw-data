from twdata.bot_repository.download import download_dataset, download_datasets
from twdata.bot_repository.process import process_dataset, process_datasets
from twdata.bot_repository.read import read_datasets


# download_datasets()
# process_datasets()
datasets = read_datasets()

for i, df in datasets.items():
    print(i)
    print(df.shape[0])
