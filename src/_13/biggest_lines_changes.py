import pandas as pd
from os import makedirs, path
from sys import setrecursionlimit

setrecursionlimit(2_000_000)

SEPARATOR = '|'

# Setup =======================================================================================================
input_path: str = "./src/_13/input/"
output_path: str = "./src/_13/output/"

repositories_path: str = "./src/_00/input/450_Starred_Projects.csv"
large_files_commits_path: str = "./src/_10/output/large_files/"
small_files_commits_path: str = "./src/_10/output/small_files/"

# Carrega e ordena repositórios por linguagem
repositories: pd.DataFrame = pd.read_csv(repositories_path).sort_values(by='main language').reset_index(drop=True)

# DataFrames globais
large_files_commits: pd.DataFrame = pd.DataFrame()
small_files_commits: pd.DataFrame = pd.DataFrame()

# função ==============================================================================================================
def calc_lines_changes(repository_commits: pd.DataFrame, type: str = "large") -> pd.DataFrame:# Preenche NaN com 0 e calcula o Lines Balance
    # processando dados =================================================================================================
    repository_commits['Lines Balance'] = repository_commits['Lines Added'] - repository_commits['Lines Deleted']
    repository_commits_modify = repository_commits[repository_commits['Change Type'] == 'MODIFY']

    # Parte 1: Valores Máximos (Lines Balance > 0)
    filtered_df = repository_commits_modify[repository_commits_modify['Lines Balance'] > 0]

    if filtered_df.empty:
        lines_added_max = 0
        project_max = "There are no added lines"
        file_max = "There are no added lines"
        hash_max = "There are no added lines"  # Novo
    else:
        lines_added_max = filtered_df['Lines Balance'].max()
        max_row = filtered_df[filtered_df['Lines Balance'] == lines_added_max].iloc[0]
        
        project_path = max_row['Local Commit PATH']
        project_max = "/".join(project_path.split("/")[-2:])
        file_max = str(max_row['Local File PATH New'])
        hash_max = max_row['Hash']  # Novo

    # Parte 2: Valores Mínimos (Lines Balance < 0)
    filtered_df = repository_commits_modify[repository_commits_modify['Lines Balance'] < 0]

    if filtered_df.empty:
        lines_deleted_min = 0
        project_min = "There is no deleted lines"
        file_min = "There is no deleted lines"
        hash_min = "There is no deleted lines"  # Novo
    else:
        lines_deleted_min = filtered_df['Lines Balance'].min()
        min_row = filtered_df[filtered_df['Lines Balance'] == lines_deleted_min].iloc[0]
        
        project_path = min_row['Local Commit PATH']
        project_min = "/".join(project_path.split("/")[-2:])
        file_min = str(min_row['Local File PATH New'])
        hash_min = min_row['Hash']  # Novo

    # DataFrame de resultado
    result = pd.DataFrame({
        "Type": [type],
        "Project Max": [project_max],
        "File Max": [file_max],
        "Hash Max": [hash_max],  # Novo
        "Added Max": [lines_added_max],
        "Project Min": [project_min],
        "File Min": [file_min],
        "Hash Min": [hash_min],  # Novo
        "Deleted Min": [lines_deleted_min],
    })
    return result


def process_language(lang: str, large: pd.DataFrame, small: pd.DataFrame, output_path: str):
    """Processa e salva resultados por linguagem"""
    results:list[pd.DataFrame] = []
    if not large.empty:
        results.append(calc_lines_changes(large, 'large'))
    if not small.empty:
        results.append(calc_lines_changes(small, 'small'))
    
    if results:
        pd.concat(results).to_csv(f"{output_path}/per_languages/{lang}.csv", index=False)

# Processamento principal =====================================================================================
current_language: str = None
current_large: pd.DataFrame = pd.DataFrame()
current_small: pd.DataFrame = pd.DataFrame()

for i, row in repositories.iterrows():
    repo_url: str = row['url']
    language: str = row['main language']
    repo_name: str = repo_url.split('/')[-1]
    repo_owner: str = repo_url.split('/')[-2]
    repo_path: str = f"{language}/{repo_owner}~{repo_name}"
    
    # Cria diretórios necessários
    makedirs(f"{output_path}/per_project/{language}", exist_ok=True)
    makedirs(f"{output_path}/per_languages", exist_ok=True)
    
    # Atualiza acumuladores de linguagem quando muda
    if current_language and (language != current_language):
        process_language(current_language, current_large, current_small, output_path)
        current_large = pd.DataFrame()
        current_small = pd.DataFrame()
    
    current_language = language
    
    # Processa arquivos grandes
    large_path = f"{large_files_commits_path}{repo_path}.csv"
    if path.exists(large_path):
        large_df: pd.DataFrame = pd.read_csv(large_path, sep=SEPARATOR)
        current_large = pd.concat([current_large, large_df])
        large_files_commits = pd.concat([large_files_commits, large_df])
        
        # Resultado por projeto (large)
        project_result: pd.DataFrame = calc_lines_changes(large_df)
        project_result.to_csv(f"{output_path}/per_project/{repo_path}_large.csv", index=False)
    
    # Processa arquivos pequenos
    small_path = f"{small_files_commits_path}{repo_path}.csv"
    if path.exists(small_path):
        small_df: pd.DataFrame = pd.read_csv(small_path, sep=SEPARATOR)
        current_small = pd.concat([current_small, small_df])
        small_files_commits = pd.concat([small_files_commits, small_df])
        
        # Resultado por projeto (small)
        project_result: pd.DataFrame = calc_lines_changes(small_df, 'small')
        project_result.to_csv(f"{output_path}/per_project/{repo_path}_small.csv", index=False)

# Processa última linguagem
if not current_large.empty or not current_small.empty:
    process_language(current_language, current_large, current_small, output_path)

# Resultado global ============================================================================================
final_results: list[pd.DataFrame] = []
if not large_files_commits.empty:
    final_results.append(calc_lines_changes(large_files_commits))
if not small_files_commits.empty:
    final_results.append(calc_lines_changes(small_files_commits, 'small'))

if final_results:
    pd.concat(final_results).to_csv(f"{output_path}/global_results.csv", index=False)