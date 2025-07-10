from xml.parsers.expat import errors
import pandas as pd
from os import makedirs, path
import datetime

SEPARATOR = ';'

# SETUP ================================================================================================================

input_path:str = "./src/_37/input/"
output_path = "./src/_37/output/"

repositories_path:str = "./src/_00/input/avalonia.csv"
# repositories_path:str = "./src/_00/input/450-linux-pytorch.csv"
language_white_list_path: str = "./src/_12/input/white_list.csv"
percentil_path: str = "./src/_02/output/percentis_by_language_filtered.csv"
large_filtered_path:str = "./src/_35/output/large/"
small_filtered_path:str = "./src/_35/output/small/"
large_files_path:str = "./src/_03/output/"
small_files_path:str = "./src/_07/output/"

language_white_list_df = pd.read_csv(language_white_list_path)
percentil_df = pd.read_csv(percentil_path)

repositories:pd.DataFrame = pd.read_csv(repositories_path)
# print(repositories)

# ======================================================================================================================

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
            # ERROR
            for large_file_name in large_file_list["path"].apply(lambda x: x.split("/")[-1]).values:
                print(large_file_name)
                if path.exists(f"{large_filtered_path}{repo_path}/{large_file_name}.csv"):
                    large_file_data = pd.read_csv(f"{large_filtered_path}{repo_path}/{large_file_name}.csv", sep=SEPARATOR)
                    if not large_file_data.empty:

                        # Process large file data here

                        # Filter only commits where file changes its classification
                        # Keep only rows where 'swapped_classification' is True or it's the first row
                        large_file_data = large_file_data[
                            (large_file_data["swapped_classification"] == True) |
                            (large_file_data.index == large_file_data.index.min())
                        ]

                        if not large_file_data.empty:
                            # Ensure numeric types for calculations
                            large_file_data["n_loc"] = pd.to_numeric(large_file_data["n_loc"], errors="coerce")
                            large_file_data["lines_balance"] = pd.to_numeric(large_file_data["lines_balance"], errors="coerce")
                            # Calculate size change with last commit
                            large_file_data["last_balance_percentage"] = ((large_file_data["n_loc"] / (large_file_data["n_loc"] - large_file_data["lines_balance"])) - 1 ) # * 100
                            # Calculate the absolute difference and percentage change of n_loc between the current and previous row
                            large_file_data["n_loc_diff"] = large_file_data["n_loc"].diff()
                            large_file_data["n_loc_pct_change"] = large_file_data["n_loc"].pct_change().fillna(0) # * 100
                            # Calc time between changes
                            large_file_data["date"] = pd.to_datetime(large_file_data["date"])
                            large_file_data["days_since_last_swap"] = large_file_data["date"].diff().dt.days.fillna(0).astype(int)

                            # Fill NaN values with 0 or appropriate default values
                            large_file_data["n_loc"] = large_file_data["n_loc"].fillna(0).astype(int)
                            large_file_data["lines_balance"] = large_file_data["lines_balance"].fillna(0).astype(int)
                            large_file_data["last_balance_percentage"] = large_file_data["last_balance_percentage"].fillna(0).astype(float)
                            large_file_data["n_loc_diff"] = large_file_data["n_loc_diff"].fillna(0).astype(int)
                            large_file_data["n_loc_pct_change"] = large_file_data["n_loc_pct_change"].fillna(0).astype(float)
                            large_file_data["days_since_last_swap"] = large_file_data["days_since_last_swap"].fillna(0).astype(int)

                        # Reorder columns as specified
                        desired_columns = [
                            "file_name",
                            "change_type", "n_loc", "lines_balance", "last_balance_percentage", "size_change",
                            "is_large", "n_loc_diff", "n_loc_pct_change", "date", "days_since_last_swap", "swapped_classification",
                            "complexity", "methods", "tokens",
                            "hash", "files_on_commit", "committer_email", "author_email"
                        ]
                        # Only keep columns that exist in the DataFrame
                        existing_columns = [col for col in desired_columns if col in large_file_data.columns]
                        large_file_data = large_file_data[existing_columns + [col for col in large_file_data.columns if col not in existing_columns]]

                        # Save processed data
                        large_file_data.to_csv(f"{output_path}large/{repo_path}/{large_file_name}.csv", sep=SEPARATOR, index=False)

    makedirs(f"{output_path}small/{repo_path}/", exist_ok=True)
    if path.exists(f"{small_filtered_path}{repo_path}.csv"):
        small_files_data = pd.read_csv(f"{small_filtered_path}{repo_path}.csv", sep=SEPARATOR)
        if not small_files_data.empty:
            ...
