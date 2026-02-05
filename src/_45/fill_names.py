import pandas as pd
from os import makedirs, path
import matplotlib.pyplot as plt

SEPARATOR = ';'

# SETUP ================================================================================================================

input_path: str = "./src/_45/input/"
output_path = "./src/_45/output/"

repositories_path: str = "./src/_00/input/450-linux-pytorch.csv"
language_white_list_path: str = "./src/_12/input/white_list.csv"
percentil_path: str = "./src/_02/output/percentis_by_language_filtered.csv"
large_filtered_path: str = "./src/_39/output/large/"
small_filtered_path: str = "./src/_39/output/small/"
large_files_path: str = "./src/_03/output/"
small_files_path: str = "./src/_07/output/"

language_white_list_df = pd.read_csv(language_white_list_path)
percentil_df = pd.read_csv(percentil_path)

repositories: pd.DataFrame = pd.read_csv(repositories_path)

# FILE =================================================================================================================

all_large_files = []

for i, row in repositories.iterrows():
    repo_url: str = row['url']
    language: str = row['main language']
    repo_name: str = repo_url.split('/')[-1]
    repo_owner: str = repo_url.split('/')[-2]
    repo_path: str = f"{language}/{repo_owner}~{repo_name}"
    print(repo_path)

    makedirs(f"{output_path}large/{repo_path}/", exist_ok=True)
    if path.exists(f"{large_filtered_path}{repo_path}"):
        large_file_list = pd.read_csv(f"{large_files_path}{repo_path}.csv", sep="|")
        if not large_file_list.empty:
            for large_file_name in large_file_list["path"].apply(lambda x: x.split("/")[-1]).values:
                all_large_files.append({"repo_path": repo_path, "file_name": large_file_name})

# Salvar todos os paths em um CSV
output_df = pd.DataFrame(all_large_files)
output_df.to_csv(f"{output_path}all_large_files.csv", index=False, sep=SEPARATOR)
print(f"Arquivo salvo em: {output_path}all_large_files.csv")
