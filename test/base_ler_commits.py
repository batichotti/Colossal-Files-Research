import pandas as pd
from os import makedirs, listdir, scandir, path
from sys import setrecursionlimit

setrecursionlimit(2_000_000)

SEPARATOR = '|'


# Setup =======================================================================================================
input_path:str = "./src/_12/input/"
output_path:str = "./src/_12/output/"

repositories_path:str = "./src/_00/input/450_Starred_Projects.csv"
large_files_commits_path:str = "./src/_11/output/large_files/"
small_files_commits_path:str = "./src/_11/output/small_files/"

repositories:pd.DataFrame = pd.read_csv(repositories_path)

large_files_commits: pd.DataFrame = pd.DataFrame()
small_files_commits: pd.DataFrame = pd.DataFrame()

def function_base(repository_commits: pd.DataFrame, type: str = "large") -> pd.DataFrame:
    # linhas removidas
    data_1: int = 0
    # contagem de zeros
    data_2: int = 0

    result: dict = {
        "Type": [type],
        "Data 1": [data_1],
        "Data 2": [data_2]
    }
    result: pd.DataFrame = pd.DataFrame(result)
    return result

# lendo csvs =======================================================================================================
last_language = repositories.loc[0, ['main language']]
large_last_language_commits: pd.DataFrame = pd.DataFrame()
small_last_language_commits: pd.DataFrame = pd.DataFrame()

for i in range(len(repositories)):
    # getting repository information
    repository, language = repositories.loc[i, ['url', 'main language']]

    makedirs(f"{output_path}/per_languages", exist_ok=True)

    repo_path: str = f"{language}/{repository.split('/')[-2]}~{repository.split('/')[-1]}"

    print(repo_path)
    if (last_language != language):
        language_result: pd.DataFrame = pd.concat(function_base(large_last_language_commits), function_base(small_last_language_commits, "small"))
        language_result.to_csv(f"{output_path}/per_languages/{language}.csv")

    if path.exists(f"{large_files_commits_path}{repo_path}"):
        repository_large_files_commit = pd.read_csv(f"{large_files_commits_path}{repo_path}", sep=SEPARATOR)
        large_files_commits = pd.concat(large_files_commits, repository_large_files_commit)
        function_base(repository_large_files_commit).to_csv(f"{output_path}{repo_path}.csv")
        if (last_language == language):
            large_last_language_commits = pd.concat(large_last_language_commits, repository_large_files_commit)

    if path.exists(f"{small_files_commits_path}{repo_path}"):
        repository_small_files_commit = pd.read_csv(f"{large_files_commits_path}{repo_path}", sep=SEPARATOR)
        small_files_commits = pd.concat(small_files_commits, repository_small_files_commit)
        function_base(repository_large_files_commit, "small").to_csv(f"{output_path}{repo_path}.csv")
        if (last_language == language):
            small_last_language_commits = pd.concat(small_last_language_commits, repository_small_files_commit)

# for all =========================================================================================================================

result : pd.DataFrame = pd.concat(function_base(large_files_commits), function_base(small_files_commits, "small"))
result.to_csv(f"{output_path}/result.csv", index=False)
