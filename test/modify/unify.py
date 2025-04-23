import pandas as pd
from os import makedirs, path
from sys import setrecursionlimit
import numpy as np
import datetime

setrecursionlimit(2_000_000)

SEPARATOR = '|'

# Setup =======================================================================================================
input_path: str = "./test/modify/input/"
output_path: str = "./test/modify/output/"

repositories_path: str = "./src/_00/input/450_Starred_Projects.csv"
modify_path: str = "./test/modify/output/per_project/"
language_white_list_path: str = "./src/_12/input/white_list.csv"
percentil_path: str = "./src/_02/output/percentis_by_language_filtered.csv"
large_files_commits_path: str = "./src/_10/output/large_files/"
small_files_commits_path: str = "./src/_10/output/small_files/"

# Carrega e ordena repositórios por linguagem
repositories: pd.DataFrame = pd.read_csv(repositories_path).sort_values(by='main language').reset_index(drop=True)
percentil_df: pd.DataFrame = pd.read_csv(percentil_path)
language_white_list_df: pd.DataFrame = pd.read_csv(language_white_list_path)

# DataFrames globais
large_files_commits: pd.DataFrame = pd.DataFrame()
small_files_commits: pd.DataFrame = pd.DataFrame()

def calc_metrics(df:pd.DataFrame) -> pd.DataFrame:
    groups = df.groupby("Dataset")
    dataset_large = groups.get_group("large") if "large" in groups.groups else pd.DataFrame()
    dataset_small = groups.get_group("small") if "small" in groups.groups else pd.DataFrame()
    large_commits = 0
    large_only_added = 0
    large_amount_mean = 0
    large_amount_median = 0
    large_life_time_mean = 0
    large_life_time_median = 0
    large_interval_mean = 0
    large_interval_median = 0
    large_deleted_total = 0
    large_deleted_amount_mean = 0
    large_deleted_amount_median = 0
    large_deleted_life_time_mean = 0
    large_deleted_life_time_median = 0
    large_deleted_interval_mean = 0
    large_deleted_interval_median = 0
    if not dataset_large.empty:
        large_commits = dataset_large["Modifications' Amount"].sum()
        large_only_added = len(dataset_large[dataset_large["Only Added?"] == True])
        large_amount_mean = dataset_large[dataset_large["Only Added?"] == False]["Modifications' Amount"].mean()
        large_amount_median = dataset_large[dataset_large["Only Added?"] == False]["Modifications' Amount"].median()
        large_life_time_mean = dataset_large[dataset_large["Only Added?"] == False]["Life Time (day)"].mean()
        large_life_time_median = dataset_large[dataset_large["Only Added?"] == False]["Life Time (day)"].median()
        large_interval_mean = dataset_large[dataset_large["Only Added?"] == False]["Modifications' Interval (day)"].mean()
        large_interval_median = dataset_large[dataset_large["Only Added?"] == False]["Modifications' Interval (day)"].median()
        large_deleted_total = len(dataset_large[dataset_large["Deleted?"] == True])
        large_deleted_amount_mean = dataset_large[dataset_large["Deleted?"] == True]["Modifications' Amount"].mean()
        large_deleted_amount_median = dataset_large[dataset_large["Deleted?"] == True]["Modifications' Amount"].median()
        large_deleted_life_time_mean = dataset_large[dataset_large["Deleted?"] == True]["Life Time (day)"].mean()
        large_deleted_life_time_median = dataset_large[dataset_large["Deleted?"] == True]["Life Time (day)"].median()
        large_deleted_interval_mean = dataset_large[dataset_large["Deleted?"] == True]["Modifications' Interval (day)"].mean()
        large_deleted_interval_median = dataset_large[dataset_large["Deleted?"] == True]["Modifications' Interval (day)"].median()

    small_commits = 0
    small_only_added = 0
    small_amount_mean = 0
    small_amount_median = 0
    small_life_time_mean = 0
    small_life_time_median = 0
    small_interval_mean = 0
    small_interval_median = 0
    small_deleted_total = 0
    small_deleted_amount_mean = 0
    small_deleted_amount_median = 0
    small_deleted_life_time_mean = 0
    small_deleted_life_time_median = 0
    small_deleted_interval_mean = 0
    small_deleted_interval_median = 0
    if not dataset_small.empty:
        small_commits = dataset_small["Modifications' Amount"].sum()
        small_only_added = len(dataset_small[dataset_small["Only Added?"] == True])
        small_amount_mean = dataset_small[dataset_small["Only Added?"] == False]["Modifications' Amount"].mean()
        small_amount_median = dataset_small[dataset_small["Only Added?"] == False]["Modifications' Amount"].median()
        small_life_time_mean = dataset_small[dataset_small["Only Added?"] == False]["Life Time (day)"].mean()
        small_life_time_median = dataset_small[dataset_small["Only Added?"] == False]["Life Time (day)"].median()
        small_interval_mean = dataset_small[dataset_small["Only Added?"] == False]["Modifications' Interval (day)"].mean()
        small_interval_median = dataset_small[dataset_small["Only Added?"] == False]["Modifications' Interval (day)"].median()
        small_deleted_total = len(dataset_small[dataset_small["Deleted?"] == True])
        small_deleted_amount_mean = dataset_small[dataset_small["Deleted?"] == True]["Modifications' Amount"].mean()
        small_deleted_amount_median = dataset_small[dataset_small["Deleted?"] == True]["Modifications' Amount"].median()
        small_deleted_life_time_mean = dataset_small[dataset_small["Deleted?"] == True]["Life Time (day)"].mean()
        small_deleted_life_time_median = dataset_small[dataset_small["Deleted?"] == True]["Life Time (day)"].median()
        small_deleted_interval_mean = dataset_small[dataset_small["Deleted?"] == True]["Modifications' Interval (day)"].mean()
        small_deleted_interval_median = dataset_small[dataset_small["Deleted?"] == True]["Modifications' Interval (day)"].median()

    result = {
        "Large Commits": [large_commits],
        "Large Only Added": [large_only_added],
        "Large Amount Mean": [large_amount_mean],
        "Large Amount Median": [large_amount_median],
        "Large Life Time Mean": [large_life_time_mean],
        "Large Life Time Median": [large_life_time_median],
        "Large Interval Mean": [large_interval_mean],
        "Large Interval Median": [large_interval_median],
        "Large Deleted Total": [large_deleted_total],
        "Large Deleted Amount Mean": [large_deleted_amount_mean],
        "Large Deleted Amount Median": [large_deleted_amount_median],
        "Large Deleted Life Time Mean": [large_deleted_life_time_mean],
        "Large Deleted Life Time Median": [large_deleted_life_time_median],
        "Large Deleted Interval Mean": [large_deleted_interval_mean],
        "Large Deleted Interval Median": [large_deleted_interval_median],

        "Small Commits": [small_commits],
        "Small Only Added": [small_only_added],
        "Small Amount Mean": [small_amount_mean],
        "Small Amount Median": [small_amount_median],
        "Small Life Time Mean": [small_life_time_mean],
        "Small Life Time Median": [small_life_time_median],
        "Small Interval Mean": [small_interval_mean],
        "Small Interval Median": [small_interval_median],
        "Small Deleted Total": [small_deleted_total],
        "Small Deleted Amount Mean": [small_deleted_amount_mean],
        "Small Deleted Amount Median": [small_deleted_amount_median],
        "Small Deleted Life Time Mean": [small_deleted_life_time_mean],
        "Small Deleted Life Time Median": [small_deleted_life_time_median],
        "Small Deleted Interval Mean": [small_deleted_interval_mean],
        "Small Deleted Interval Median": [small_deleted_interval_median],
    }
    return pd.DataFrame(result)

# ================================================================================================================
all_df = pd.DataFrame()

current_language: str = None
current_df: pd.DataFrame = pd.DataFrame()

for i, row in repositories.iterrows():
    repo_url: str = row['url']
    language: str = row['main language']
    repo_name: str = repo_url.split('/')[-1]
    repo_owner: str = repo_url.split('/')[-2]
    repo_path: str = f"{language}/{repo_owner}~{repo_name}"

    print(repo_path)

    # Cria diretórios necessários
    makedirs(f"{output_path}general/per_project/{language}", exist_ok=True)
    makedirs(f"{output_path}general/per_language/", exist_ok=True)

    # Atualiza acumuladores de linguagem quando muda
    if current_language and (language != current_language):
        calc_metrics(current_df).to_csv(f"{output_path}general/per_language/{current_language}.csv", index=False)
        all_df = pd.concat([all_df, current_df])
        current_df = pd.DataFrame()

    current_language = language

    # Processa por projeto
    df = pd.DataFrame()
    if path.exists(f"{modify_path}{repo_path}.csv"):
        df = pd.read_csv(f"{modify_path}{repo_path}.csv")
        calc_metrics(df).to_csv(f"{output_path}general/per_project/{repo_path}.csv", index=False)
        current_df = pd.concat([current_df, df])

# Processa última linguagem
if not current_df.empty:
    calc_metrics(current_df).to_csv(f"{output_path}general/per_language/{current_language}.csv", index=False)
    all_df = pd.concat([all_df, current_df])

# Resultado global ============================================================================================
if not all_df.empty:
    calc_metrics(all_df).to_csv(f"{output_path}general/global_results.csv", index=False)

