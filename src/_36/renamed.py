import pandas as pd
from os import makedirs, path
import datetime

SEPARATOR = ';'

# SETUP ================================================================================================================

input_path:str = "./src/_36/input/"
output_path = "./src/_36/output/"

# repositories_path:str = "./src/_00/input/avalonia.csv"
repositories_path:str = "./src/_00/input/450-linux-pytorch.csv"
language_white_list_path: str = "./src/_12/input/white_list.csv"
percentil_path: str = "./src/_02/output/percentis_by_language_filtered.csv"
large_filtered_path:str = "./src/_34/output/large/"
small_filtered_path:str = "./src/_34/output/small/"

language_white_list_df = pd.read_csv(language_white_list_path)
percentil_df = pd.read_csv(percentil_path)

repositories:pd.DataFrame = pd.read_csv(repositories_path)
# print(repositories)

# ======================================================================================================================

count_large = 0
count_small = 0

for i, row in repositories.iterrows():
    repo_url: str = row['url']
    language: str = row['main language']
    repo_name: str = repo_url.split('/')[-1]
    repo_owner: str = repo_url.split('/')[-2]
    repo_path: str = f"{language}/{repo_owner}~{repo_name}"
    print(repo_path)

    renamed: pd.DataFrame = pd.DataFrame()

    makedirs(f"{output_path}large/{language}", exist_ok=True)
    if path.exists(f"{large_filtered_path}{repo_path}.csv"):
        large_files_data = pd.read_csv(f"{large_filtered_path}{repo_path}.csv", sep=SEPARATOR)
        if not large_files_data.empty:
            large_grouped = large_files_data.groupby("File Name")
            for _, large_file in large_grouped:
                if large_file["Change Type"].iloc[0] == "RENAME":
                    count_large += 1

                    old_path = large_file["Local File PATH Old"].iloc[0]
                    new_path = large_file["Local File PATH New"].iloc[0]

                    renamed = pd.concat([renamed, pd.DataFrame([{
                        "repo_path": repo_path,
                        "path": old_path,
                        "original_path": new_path # <- Before Renaming
                    }])], ignore_index=True)

    if not renamed.empty:
        renamed.to_csv(f"{output_path}large/{repo_path}.csv", index=False)

    renamed = pd.DataFrame()
    makedirs(f"{output_path}small/{language}", exist_ok=True)
    if path.exists(f"{small_filtered_path}{repo_path}.csv"):
        small_files_data = pd.read_csv(f"{small_filtered_path}{repo_path}.csv", sep=SEPARATOR)
        if not small_files_data.empty:
            small_grouped = small_files_data.groupby("File Name")
            for _, small_file in small_grouped:
                if small_file["Change Type"].iloc[0] == "RENAME":
                    count_small += 1

                    old_path = small_file["Local File PATH Old"].iloc[0]
                    new_path = small_file["Local File PATH New"].iloc[0]

                    renamed = pd.concat([renamed, pd.DataFrame([{
                        "repo_path": repo_path,
                        "path": old_path,
                        "original_path": new_path # <- Before Renaming
                    }])], ignore_index=True)

    if not renamed.empty:
        renamed.to_csv(f"{output_path}small/{repo_path}.csv", index=False)

print(count_large)
print(count_small)
