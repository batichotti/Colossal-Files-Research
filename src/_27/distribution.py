import pandas as pd
from os import makedirs

SEPARATOR = '|'

# Setup
input_path:str = "./src/_27/input/"
output_path:str = "./src/_27/output/"

repositories_path:str = "./src/_00/input/450-linux-pytorch.csv"
cloc_path:str = "./src/_01/output/"
large_files_path:str = "./src/_03/output/"
large_files_total_per_language_path:str = "./src/_05/output/#large_files.csv"

repositories:pd.DataFrame = pd.read_csv(repositories_path)
large_files_total_per_language:pd.DataFrame = pd.read_csv(large_files_total_per_language_path)

# script
projects_total: int = 0
projects_with_large: int = 0

for i in range(len(repositories)):
    # getting repository information
    repository, language = repositories.loc[i, ['url', 'main language']]

    # makedirs(f"{output_path}{language}", exist_ok=True)
    repo_path: str = f"{language}/{repository.split('/')[-2]}~{repository.split('/')[-1]}"
    print(repo_path)

    repository_df: pd.DataFrame = pd.read_csv(f"{large_files_path}{repo_path}.csv", sep=SEPARATOR)

    projects_total += 1
    if not repository_df.empty:
        projects_with_large += 1

projects_without_large: int = projects_total - projects_with_large

result: dict = {
    "Projects TOTAL": [projects_total],
    "Projects with Large": [projects_with_large],
    "Projects without Large": [projects_without_large],
    "Percentage with Large": [projects_with_large/projects_total if projects_total else 0],
    "Percentage without Large": [projects_without_large/projects_total if projects_total else 0]
}

pd.DataFrame(result).to_csv(f"{output_path}result.csv")
