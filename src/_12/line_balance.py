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

last_language: str = repositories.loc[0, 'main language']
large_last_language_commits: pd.DataFrame = pd.DataFrame()
small_last_language_commits: pd.DataFrame = pd.DataFrame()


# função ==============================================================================================================
def calc_lines_changes(repository_commits: pd.DataFrame, type: str = "large") -> pd.DataFrame:
    # processando dados =================================================================================================
    repository_commits['Lines Balance'] = repository_commits['Lines Added'] + repository_commits['Lines Deleted']

    # Filtrando apenas os commits onde o tipo de mudança é "MODIFY"
    repository_commits_modify = repository_commits[repository_commits['Change Type'] == 'MODIFY']

    # linhas adicionadas
    lines_added_avg: int = repository_commits_modify[repository_commits_modify['Lines Balance'] > 0]['Lines Balance'].mean()
    lines_added_mean: int = repository_commits_modify[repository_commits_modify['Lines Balance'] > 0]['Lines Balance'].median()
    lines_added_max: int = repository_commits_modify[repository_commits_modify['Lines Balance'] > 0]['Lines Balance'].max()

    # linhas removidas
    lines_deleted_avg: int = repository_commits_modify[repository_commits_modify['Lines Balance'] < 0]['Lines Balance'].mean()
    lines_deleted_mean: int = repository_commits_modify[repository_commits_modify['Lines Balance'] < 0]['Lines Balance'].median()
    lines_deleted_min: int = repository_commits_modify[repository_commits_modify['Lines Balance'] < 0]['Lines Balance'].min()

    # contagem de zeros
    zero_count: int = len(repository_commits_modify[repository_commits_modify['Lines Balance'] == 0])

    # salvando resultado ===============================================================================================================
    result: dict = {
        "Type": [type],
        "Added Average": [lines_added_avg],
        "Added Mean": [lines_added_mean],
        "Added Max": [lines_added_max],
        "Deleted Average": [lines_deleted_avg],
        "Deleted Mean": [lines_deleted_mean],
        "Deleted Min": [lines_deleted_min],
        "Zero Count": [zero_count]
    }
    result: pd.DataFrame = pd.DataFrame(result)
    return result


# lendo csvs =======================================================================================================
for i in range(len(repositories)):
    # getting repository information
    repository, language = repositories.loc[i, ['url', 'main language']]

    makedirs(f"{output_path}/per_languages", exist_ok=True)
    makedirs(f"{output_path}/per_project", exist_ok=True)

    repo_path: str = f"{language}/{repository.split('/')[-2]}~{repository.split('/')[-1]}"

    repository_large_files_commit: pd.DataFrame = pd.DataFrame()
    repository_small_files_commit: pd.DataFrame = pd.DataFrame()

    print(repo_path)
    
    # if (last_language != language):
    #     language_result: pd.DataFrame = pd.DataFrame() # if temporio do mateus
    #     if path.exists(f"{large_files_commits_path}{language}"): # if temporio do mateus
    #         language_result: pd.DataFrame = pd.concat([calc_lines_changes(large_last_language_commits), calc_lines_changes(small_last_language_commits, "small")])
    #     elif path.exists(f"{small_files_commits_path}{language}"): # if temporio do mateus
    #         language_result: pd.DataFrame = calc_lines_changes(small_last_language_commits, "small")
    #     if not language_result.empty: # if temporio do mateus
    #         language_result.to_csv(f"{output_path}per_languages/{language}.csv")
    #     language_result = pd.DataFrame()
    #     large_last_language_commits = pd.DataFrame()
    #     small_last_language_commits = pd.DataFrame()
    #     last_language = language

    if path.exists(f"{large_files_commits_path}{repo_path}.csv"):
        repository_large_files_commit = pd.read_csv(f"{large_files_commits_path}{repo_path}.csv", sep=SEPARATOR)
        large_files_commits = pd.concat([large_files_commits, repository_large_files_commit])
        if (last_language == language):
            large_last_language_commits = pd.concat([large_last_language_commits, repository_large_files_commit])

    if path.exists(f"{small_files_commits_path}{repo_path}.csv"):
        repository_small_files_commit = pd.read_csv(f"{small_files_commits_path}{repo_path}.csv", sep=SEPARATOR)
        small_files_commits = pd.concat([small_files_commits, repository_small_files_commit])
        if (last_language == language):
            small_last_language_commits = pd.concat([small_last_language_commits, repository_small_files_commit])

    large_project_result: pd.DataFrame = pd.DataFrame()
    if (not repository_large_files_commit.empty):
        large_project_result: pd.DataFrame = calc_lines_changes(repository_large_files_commit)
    small_project_result: pd.DataFrame = pd.DataFrame()
    if (not repository_small_files_commit.empty):
        small_project_result: pd.DataFrame = calc_lines_changes(repository_small_files_commit, "small")
    if ((not large_project_result.empty) or (not small_project_result.empty)):
        pd.concat([large_project_result, small_project_result]).to_csv(f"{output_path}/per_project/{repo_path}.csv")

# for all =========================================================================================================================

# result: pd.DataFrame = pd.DataFrame # if temporio do mateus
# if (not large_files_commits.empty): # if temporio do mateus
#     result : pd.DataFrame = pd.concat([calc_lines_changes(large_files_commits), calc_lines_changes(small_files_commits, "small")])
# else:
#     result : pd.DataFrame = calc_lines_changes(small_files_commits, "small")
# result.to_csv(f"{output_path}/result.csv", index=False)
