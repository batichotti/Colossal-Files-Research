import pandas as pd
from os import makedirs

SEPARATOR = '|'

# Setup
input_path:str = "./src/_28/input/"
output_path:str = "./src/_28/output/"

repositories_path:str = "./src/_00/input/450-linux-pytorch.csv"
cloc_path:str = "./src/_01/output/"

repositories:pd.DataFrame = pd.read_csv(repositories_path)

repositories_concat: pd.DataFrame = pd.DataFrame()

# script

for i in range(len(repositories)):
    # getting repository information
    repository, language = repositories.loc[i, ['url', 'main language']]

    # makedirs(f"{output_path}{language}", exist_ok=True)
    repo_path: str = f"{language}/{repository.split('/')[-2]}~{repository.split('/')[-1]}"
    print(repo_path)

    repository_df: pd.DataFrame = pd.read_csv(f"{cloc_path}{repo_path}.csv", sep=SEPARATOR)

    repositories_concat = pd.concat([repositories_concat, repository_df])

files_total = len(repositories_concat.groupby('path'))
extensions_total = len(repositories_concat.groupby('language'))
lines_total = repositories_concat['code'].sum()

result: dict = {
    "Files TOTAL": [files_total],
    "Extensions TOTAL": [extensions_total],
    "Lines TOTAL": [lines_total],
}

makedirs(output_path, exist_ok=True)
pd.DataFrame(result).to_csv(f"{output_path}result.csv")
