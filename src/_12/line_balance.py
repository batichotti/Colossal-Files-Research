import pandas as pd
from os import makedirs, listdir, scandir, path
from sys import setrecursionlimit

setrecursionlimit(300000)

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


# lendo csvs =======================================================================================================
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


# processando dados =================================================================================================
large_files_commits['Lines Balance'] = large_files_commits['Lines Added'] + large_files_commits['Lines Deleted']
small_files_commits['Lines Balance'] = small_files_commits['Lines Added'] + small_files_commits['Lines Deleted']

# Filtrando apenas os commits onde o tipo de mudança é "MODIFY"
large_files_commits_modify = large_files_commits[large_files_commits['Change Type'] == 'MODIFY']
small_files_commits_modify = small_files_commits[small_files_commits['Change Type'] == 'MODIFY']

# linhas adicionadas
large_lines_added_avg: int = large_files_commits_modify[large_files_commits_modify['Lines Balance'] > 0]['Lines Balance'].mean()
large_lines_added_mean: int = large_files_commits_modify[large_files_commits_modify['Lines Balance'] > 0]['Lines Balance'].median()
large_lines_added_max: int = large_files_commits_modify[large_files_commits_modify['Lines Balance'] > 0]['Lines Balance'].max()

small_lines_added_avg: int = small_files_commits_modify[small_files_commits_modify['Lines Balance'] > 0]['Lines Balance'].mean()
small_lines_added_mean: int = small_files_commits_modify[small_files_commits_modify['Lines Balance'] > 0]['Lines Balance'].median()
small_lines_added_max: int = small_files_commits_modify[small_files_commits_modify['Lines Balance'] > 0]['Lines Balance'].max()

# linhas removidas
large_lines_deleted_avg: int = large_files_commits_modify[large_files_commits_modify['Lines Balance'] < 0]['Lines Balance'].mean()
large_lines_deleted_mean: int = large_files_commits_modify[large_files_commits_modify['Lines Balance'] < 0]['Lines Balance'].median()
large_lines_deleted_min: int = large_files_commits_modify[large_files_commits_modify['Lines Balance'] < 0]['Lines Balance'].min()

small_lines_deleted_avg: int = small_files_commits_modify[small_files_commits_modify['Lines Balance'] < 0]['Lines Balance'].mean()
small_lines_deleted_mean: int = small_files_commits_modify[small_files_commits_modify['Lines Balance'] < 0]['Lines Balance'].median()
small_lines_deleted_min: int = small_files_commits_modify[small_files_commits_modify['Lines Balance'] < 0]['Lines Balance'].min()

# contagem de zeros
large_zero_count: int = len(large_files_commits_modify[large_files_commits_modify['Lines Balance'] == 0])
small_zero_count: int = len(small_files_commits_modify[small_files_commits_modify['Lines Balance'] == 0])


# salvando resultado ===============================================================================================================
result: dict = {
    "Type": ["large", "small"],
    "Added Average": [large_lines_added_avg, small_lines_added_avg],
    "Added Mean": [large_lines_added_mean, small_lines_added_mean],
    "Added Max": [large_lines_added_max, small_lines_added_max],
    "Deleted Average": [large_lines_deleted_avg, small_lines_deleted_avg],
    "Deleted Mean": [large_lines_deleted_mean, small_lines_deleted_mean],
    "Deleted Min": [large_lines_deleted_min, small_lines_deleted_min],
    "Zero Count": [large_zero_count, small_zero_count]
}
result: pd.DataFrame = pd.DataFrame(result)
result.to_csv(f"{output_path}/result.csv", index=False)
