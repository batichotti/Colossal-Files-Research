import pandas as pd
from os import makedirs, path
from sys import setrecursionlimit

setrecursionlimit(2_000_000)

SEPARATOR = '|'

# Setup
input_path:str = "./src/_XX/input/"
output_path:str = "./src/_XX/output/"

repositories_path:str = "./src/_00/input/450_Starred_Projects.csv"
large_files_commits_path:str = "./src/_11/output/large_files/"
small_files_commits_path:str = "./src/_11/output/small_files/"

repositories:pd.DataFrame = pd.read_csv(repositories_path)

large_files_commits: pd.DataFrame = pd.DataFrame()
samll_files_commits: pd.DataFrame = pd.DataFrame()

for i in range(len(repositories)):
    # getting repository information
    repository, language = repositories.loc[i, ['url', 'main language']]

    makedirs(f"{output_path}", exist_ok=True)

    repo_path: str = f"{language}/{repository.split('/')[-2]}~{repository.split('/')[-1]}"

    print(repo_path)

    if path.exists(f"{large_files_commits_path}{repo_path}"):
        repository_large_files_commit = pd.read_csv(f"{large_files_commits_path}{repo_path}", sep=SEPARATOR)
        large_files_commits = pd.concat(large_files_commits, repository_large_files_commit)
    if path.exists(f"{small_files_commits_path}{repo_path}"):
        repository_small_files_commit = pd.read_csv(f"{large_files_commits_path}{repo_path}", sep=SEPARATOR)
        small_files_commits = pd.concat(small_files_commits, repository_small_files_commit)
