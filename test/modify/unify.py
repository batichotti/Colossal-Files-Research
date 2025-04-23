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
        large_commits = dataset_large[dataset_large["File Category"] == "large"]["Modifications' Amount"].sum()
        large_only_added = len(dataset_large[(dataset_large["Only Added?"] == True) & (dataset_large["File Category"] == "large")])
        large_amount_mean = dataset_large[(dataset_large["Only Added?"] == False) & (dataset_large["File Category"] == "large")]["Modifications' Amount"].mean()
        large_amount_median = dataset_large[(dataset_large["Only Added?"] == False) & (dataset_large["File Category"] == "large")]["Modifications' Amount"].median()
        large_life_time_mean = dataset_large[(dataset_large["Only Added?"] == False) & (dataset_large["File Category"] == "large")]["Life Time (second)"].mean() / 86400
        large_life_time_median = dataset_large[(dataset_large["Only Added?"] == False) & (dataset_large["File Category"] == "large")]["Life Time (second)"].median() / 86400
        large_interval_mean = dataset_large[(dataset_large["Only Added?"] == False) & (dataset_large["File Category"] == "large")]["Modifications' Interval (second)"].mean() / 86400
        large_interval_median = dataset_large[(dataset_large["Only Added?"] == False) & (dataset_large["File Category"] == "large")]["Modifications' Interval (second)"].median() / 86400
        large_deleted_total = len(dataset_large[(dataset_large["Deleted?"] == True) & (dataset_large["File Category"] == "large")])
        large_deleted_amount_mean = dataset_large[(dataset_large["Deleted?"] == True) & (dataset_large["File Category"] == "large")]["Modifications' Amount"].mean()
        large_deleted_amount_median = dataset_large[(dataset_large["Deleted?"] == True) & (dataset_large["File Category"] == "large")]["Modifications' Amount"].median()
        large_deleted_life_time_mean = dataset_large[(dataset_large["Deleted?"] == True) & (dataset_large["File Category"] == "large")]["Life Time (second)"].mean() / 86400
        large_deleted_life_time_median = dataset_large[(dataset_large["Deleted?"] == True) & (dataset_large["File Category"] == "large")]["Life Time (second)"].median() / 86400
        large_deleted_interval_mean = dataset_large[(dataset_large["Deleted?"] == True) & (dataset_large["File Category"] == "large")]["Modifications' Interval (second)"].mean() / 86400
        large_deleted_interval_median = dataset_large[(dataset_large["Deleted?"] == True) & (dataset_large["File Category"] == "large")]["Modifications' Interval (second)"].median() / 86400

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
        small_commits = dataset_small[dataset_small["File Category"] == "small"]["Modifications' Amount"].sum()
        small_only_added = len(dataset_small[(dataset_small["Only Added?"] == True) & (dataset_small["File Category"] == "small")])
        small_amount_mean = dataset_small[(dataset_small["Only Added?"] == False) & (dataset_small["File Category"] == "small")]["Modifications' Amount"].mean()
        small_amount_median = dataset_small[(dataset_small["Only Added?"] == False) & (dataset_small["File Category"] == "small")]["Modifications' Amount"].median()
        small_life_time_mean = dataset_small[(dataset_small["Only Added?"] == False) & (dataset_small["File Category"] == "small")]["Life Time (second)"].mean() / 86400
        small_life_time_median = dataset_small[(dataset_small["Only Added?"] == False) & (dataset_small["File Category"] == "small")]["Life Time (second)"].median() / 86400
        small_interval_mean = dataset_small[(dataset_small["Only Added?"] == False) & (dataset_small["File Category"] == "small")]["Modifications' Interval (second)"].mean() / 86400
        small_interval_median = dataset_small[(dataset_small["Only Added?"] == False) & (dataset_small["File Category"] == "small")]["Modifications' Interval (second)"].median() / 86400
        small_deleted_total = len(dataset_small[(dataset_small["Deleted?"] == True) & (dataset_small["File Category"] == "small")])
        small_deleted_amount_mean = dataset_small[(dataset_small["Deleted?"] == True) & (dataset_small["File Category"] == "small")]["Modifications' Amount"].mean()
        small_deleted_amount_median = dataset_small[(dataset_small["Deleted?"] == True) & (dataset_small["File Category"] == "small")]["Modifications' Amount"].median()
        small_deleted_life_time_mean = dataset_small[(dataset_small["Deleted?"] == True) & (dataset_small["File Category"] == "small")]["Life Time (second)"].mean() / 86400
        small_deleted_life_time_median = dataset_small[(dataset_small["Deleted?"] == True) & (dataset_small["File Category"] == "small")]["Life Time (second)"].median() / 86400
        small_deleted_interval_mean = dataset_small[(dataset_small["Deleted?"] == True) & (dataset_small["File Category"] == "small")]["Modifications' Interval (second)"].mean() / 86400
        small_deleted_interval_median = dataset_small[(dataset_small["Deleted?"] == True) & (dataset_small["File Category"] == "small")]["Modifications' Interval (second)"].median() / 86400

    result = {
        "Type": ["Large", "Small"],
        "Commits": [large_commits, small_commits],
        "Only Added": [large_only_added, small_only_added],
        "Amount Mean": [large_amount_mean, small_amount_mean],
        "Amount Median": [large_amount_median, small_amount_median],
        "Life Time Mean": [large_life_time_mean, small_life_time_mean],
        "Life Time Median": [large_life_time_median, small_life_time_median],
        "Interval Mean": [large_interval_mean, small_interval_mean],
        "Interval Median": [large_interval_median, small_interval_median],
        "Deleted Total": [large_deleted_total, small_deleted_total],
        "Deleted Amount Mean": [large_deleted_amount_mean, small_deleted_amount_mean],
        "Deleted Amount Median": [large_deleted_amount_median, small_deleted_amount_median],
        "Deleted Life Time Mean": [large_deleted_life_time_mean, small_deleted_life_time_mean],
        "Deleted Life Time Median": [large_deleted_life_time_median, small_deleted_life_time_median],
        "Deleted Interval Mean": [large_deleted_interval_mean, small_deleted_interval_mean],
        "Deleted Interval Median": [large_deleted_interval_median, small_deleted_interval_median],
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
