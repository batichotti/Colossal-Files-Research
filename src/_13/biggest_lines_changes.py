import pandas as pd
from os import makedirs, listdir, scandir, path
from sys import setrecursionlimit

setrecursionlimit(2_000_000)

SEPARATOR = '|'


# Setup =======================================================================================================
input_path:str = "./src/_13/input/"
output_path:str = "./src/_13/output/"

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
    repository_commits['Lines Balance'] = repository_commits['Lines Added'] - repository_commits['Lines Deleted']
    repository_commits_modify = repository_commits[repository_commits['Change Type'] == 'MODIFY']

    # Filtrando apenas commits com "Lines Balance" positivo
    filtered_df = repository_commits_modify[repository_commits_modify['Lines Balance'] > 0]

    if filtered_df.empty:
        lines_added_max = 0
        project_max = "There is no added lines"
        file_max = "There is no added lines"
    else:
        lines_added_max = filtered_df['Lines Balance'].max()
        max_idx = filtered_df['Lines Balance'].idxmax()

        # Se houver múltiplos índices, pegar o primeiro
        if isinstance(max_idx, pd.Series):
            max_idx = max_idx.iloc[0]

        # Garantir que project_max seja uma string
        project_max = repository_commits_modify.loc[max_idx, "Local Commit PATH"]
        if isinstance(project_max, pd.Series):
            project_max = project_max.iloc[0]

        project_max = "/".join(str(project_max).split("/")[-2:0]) if "/" in str(project_max) else str(project_max)

        # Garantir que file_max seja uma string
        file_max = repository_commits_modify.loc[max_idx, "Local File PATH New"]
        if isinstance(file_max, pd.Series):
            file_max = file_max.iloc[0]
        file_max = str(file_max)

    # Filtrando commits com "Lines Balance" negativo
    filtered_df = repository_commits_modify[repository_commits_modify['Lines Balance'] < 0]

    if filtered_df.empty:
        lines_deleted_min = 0
        project_min = "There is no deleted lines"
        file_min = "There is no deleted lines"
    else:
        lines_deleted_min = filtered_df['Lines Balance'].min()
        min_idx = filtered_df['Lines Balance'].idxmin()

        # Se houver múltiplos índices, pegar o primeiro
        if isinstance(min_idx, pd.Series):
            min_idx = min_idx.iloc[0]

        # Garantir que project_min seja uma string
        project_min = repository_commits_modify.loc[min_idx, "Local Commit PATH"]
        if isinstance(project_min, pd.Series):
            project_min = project_min.iloc[0]

        project_min = "/".join(str(project_min).split("/")[-2: 0]) if "/" in str(project_min) else str(project_min)

        # Garantir que file_min seja uma string
        file_min = repository_commits_modify.loc[min_idx, "Local File PATH New"]
        if isinstance(file_min, pd.Series):
            file_min = file_min.iloc[0]
        file_min = str(file_min)

    # Retornando os resultados como DataFrame
    result = pd.DataFrame({
        "Type": [type],
        "Project Max": [project_max],
        "File Max": [file_max],
        "Added Max": [lines_added_max],
        "Project Min": [project_min],
        "File Min": [file_min],
        "Deleted Min": [lines_deleted_min],
    })
    return result


# lendo csvs =======================================================================================================
for i in range(len(repositories)):
    # getting repository information
    repository, language = repositories.loc[i, ['url', 'main language']]

    makedirs(f"{output_path}/per_languages/{language}", exist_ok=True)
    makedirs(f"{output_path}/per_project/{language}", exist_ok=True)

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
    #         language_result.to_csv(f"{output_path}per_languages/{language}.csv", index=False)
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
        pd.concat([large_project_result, small_project_result]).to_csv(f"{output_path}/per_project/{repo_path}.csv", index=False)

# for all =========================================================================================================================

result: pd.DataFrame = pd.DataFrame # if temporio do mateus
if (not large_files_commits.empty): # if temporio do mateus
    result : pd.DataFrame = pd.concat([calc_lines_changes(large_files_commits), calc_lines_changes(small_files_commits, "small")])
else:
    result : pd.DataFrame = calc_lines_changes(small_files_commits, "small")
result.to_csv(f"{output_path}/result.csv", index=False)
