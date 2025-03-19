import pandas as pd
import numpy as np
from os import makedirs, path
from sys import setrecursionlimit

setrecursionlimit(2_000_000)

SEPARATOR = '|'

# Setup =======================================================================================================
input_path: str = "./src/_14/input/"
output_path: str = "./src/_14/output/"

repositories_path: str = "./src/_00/input/450_Starred_Projects.csv"
large_files_list_path: str = "./src/_03/output/"
large_files_commits_path: str = "./src/_10/output/large_files/"
small_files_commits_path: str = "./src/_10/output/small_files/"

# Carrega e ordena repositórios por linguagem
repositories: pd.DataFrame = pd.read_csv(repositories_path).sort_values(by='main language').reset_index(drop=True)

# DataFrames globais
large_files_commits: pd.DataFrame = pd.DataFrame()
small_files_commits: pd.DataFrame = pd.DataFrame()
large_files_list_geral: pd.DataFrame = pd.DataFrame()

# Funções auxiliares =========================================================================================

def together_change(repository_commits: pd.DataFrame, large_files_list_df: pd.DataFrame, change_type: str = "large") -> pd.DataFrame:
    """Detecta quais commits de arquivos grandes tiveram mudanças com outros arquivos"""

    # ver com quantos arquivos? quanto eram grandes? quantos eram pequenos
    # agrupar o df "repository_commits" pela coluna ["Hash"] para faze as análises pelas hashs
    # ve se ou na coluna "Local File PATH Old" ou na "Local File PATH New" existe algum arquivo grande em large_files_list_df['File Path']
    # caso exista verificar quantos elementos da mesma hash diferentes do arquivo analisados tem
    # se existir algum outro elemento anota quantos elementos existem
    # bem como verificar quais desses elementos estão na lista de arquivos grandes
    # salvar esses dois valores separados
    # fazer isso para todas as hashs
    # calcular media/avg, mediana, e porcentagem pelo total e total para quantos commits tiveram mudança de um arquivo grande com outro arquivo
    # calcular mesmas medias para quantas dessas mudanças foram com arquivos grandes
    # quantas foram com arquivos não grandes
    # por fim quantas foram com arquivos grandes e tambem com arquivos não grandes

    def path_correction(file_path: str) -> str:
        """Corrige o path para analise em together_change()"""
        # Implementação fictícia para cálculo da métrica
        return "/".join(file_path.split("/")[6:])
    
    large_files_list_df['File Path'] = large_files_list_df["path"].apply(lambda x: path_correction(x))
    large_files_set: set[str] = set(large_files_list_df['File Path'])

    # Group commits by hash
    grouped = repository_commits.groupby('Hash')
    repo_commits_total = len(repository_commits["Hash"].unique())
    
    # Collect metrics for commits with large files
    total_commits: int = 0
    total_with_large: int = 0
    total_with_small: int = 0
    totals: list[int] = []
    large_counts: list[int] = []
    small_counts: list[int] = []
    
    for hash_val, group in grouped:
        unique_files: set[str] = set()
        for _, row in group.iterrows():
            old = row['Local File PATH Old']
            new = row['Local File PATH New']
            if old:
                unique_files.add(old)
            if new:
                unique_files.add(new)
        if 'new file' in unique_files:
            unique_files.remove('new file') # remove string de new file

        # Check if any of the files are large
        if unique_files & large_files_set:
            first_large = next(iter(unique_files & large_files_set)) # pega o primeiro elemento
            remaining_files = unique_files.copy()
            remaining_files.remove(first_large) # retira o primeiro large file das contas

            total: int = len(remaining_files)
            large: int = len(remaining_files & large_files_set)
            small: int = total - large
            # files count
            totals.append(total)
            large_counts.append(large)
            small_counts.append(small)
            # commits count
            total_commits += 1
            if large:
                total_with_large += 1
            if small:
                total_with_small += 1
            # else:
            #     print(hash_val)

    # Compute statistics
    # total_commits = len(totals)
    if total_commits == 0:
        # Return default values if no commits with large files
        result = {
            "Type": [change_type],
            "#Commits Anal": [repo_commits_total],
            "Together Mean": [0],
            "Together Median": [0],
            "Together TOTAL": [0],
            "Together Percentage": [0],
            "Together with Large Mean": [0],
            "Together with Large Median": [0],
            "Together with Large TOTAL": [0],
            "Together with Large Percentage": [0],
            "Together with Small Mean": [0],
            "Together with Small Median": [0],
            "Together with Small TOTAL": [0],
            "Together with Small Percentage": [0]
        }
        return pd.DataFrame(result)

    else:
        # Calculate metrics
        together_mean = np.mean(totals)
        together_median = np.median(totals)
        together_percentage = (total_commits / repo_commits_total) * 100
        
        together_large_mean = np.mean(large_counts)
        together_large_median = np.median(large_counts)
        together_large_percentage = (total_with_large / total_commits) * 100
        
        together_small_mean = np.mean(small_counts)
        together_small_median = np.median(small_counts)
        together_small_percentage = (total_with_small / total_commits) * 100

        # Build result dictionary
        result = {
            "Type": [change_type],
            "#Commits Anal": [repo_commits_total],
            "Together Mean": [together_mean],
            "Together Median": [together_median],
            "Together TOTAL": [total_commits],
            "Together Percentage": [together_percentage],
            "Together with Large Mean": [together_large_mean],
            "Together with Large Median": [together_large_median],
            "Together with Large TOTAL": [total_with_large],
            "Together with Large Percentage": [together_large_percentage],
            "Together with Small Mean": [together_small_mean],
            "Together with Small Median": [together_small_median],
            "Together with Small TOTAL": [total_with_small],
            "Together with Small Percentage": [together_small_percentage]
        }
    
    return pd.DataFrame(result)

def process_language(lang: str, large: pd.DataFrame, small: pd.DataFrame, large_files_list_df:pd.DataFrame, output_path: str):
    """Processa e salva resultados por linguagem"""
    results:list[pd.DataFrame] = []
    if not large.empty:
        results.append(together_change(large, large_files_list_df, 'large'))
    if not small.empty:
        results.append(together_change(small, large_files_list_df, 'small'))
    
    if results:
        pd.concat(results).to_csv(f"{output_path}/per_languages/{lang}.csv", index=False)

# Processamento principal =====================================================================================
current_language: str = None
current_large: pd.DataFrame = pd.DataFrame()
current_small: pd.DataFrame = pd.DataFrame()
current_large_list: pd.DataFrame = pd.DataFrame()

for i, row in repositories.iterrows():
    repo_url: str = row['url']
    language: str = row['main language']
    repo_name: str = repo_url.split('/')[-1]
    repo_owner: str = repo_url.split('/')[-2]
    repo_path: str = f"{language}/{repo_owner}~{repo_name}"

    print(repo_path)

    # Cria diretórios necessários
    makedirs(f"{output_path}/per_project/{language}", exist_ok=True)
    makedirs(f"{output_path}/per_languages", exist_ok=True)


    # Atualiza acumuladores de linguagem quando muda
    if current_language and (language != current_language):
        process_language(current_language, current_large, current_small, current_large_list, output_path)
        current_large_list = pd.DataFrame()
        current_large = pd.DataFrame()
        current_small = pd.DataFrame()
    
    current_language = language

    large_list_df: pd.DataFrame = pd.read_csv(f"{large_files_list_path}{repo_path}.csv", sep=SEPARATOR)
    current_large_list = pd.concat([current_large_list, large_list_df])
    large_files_list_geral = pd.concat([large_files_list_geral, large_list_df])
    
    # Processa arquivos grandes
    large_df: pd.DataFrame = pd.DataFrame()
    large_path = f"{large_files_commits_path}{repo_path}.csv"
    if path.exists(large_path):
        large_df: pd.DataFrame = pd.read_csv(large_path, sep=SEPARATOR)
        current_large = pd.concat([current_large, large_df])
        large_files_commits = pd.concat([large_files_commits, large_df])
    
    # Processa arquivos pequenos
    small_path = f"{small_files_commits_path}{repo_path}.csv"
    small_df: pd.DataFrame = pd.DataFrame()
    if path.exists(small_path):
        small_df: pd.DataFrame = pd.read_csv(small_path, sep=SEPARATOR)
        current_small = pd.concat([current_small, small_df])
        small_files_commits = pd.concat([small_files_commits, small_df])
    
    project_results: list[pd.DataFrame] = []
    if not large_df.empty:
        project_results.append(together_change(large_df, large_list_df))
    if not small_df.empty:
        project_results.append(together_change(small_df, large_list_df, 'small'))

    if project_results:
        pd.concat(project_results).to_csv(f"{output_path}/per_project/{repo_path}.csv", index=False)

# Processa última linguagem
if not current_large.empty or not current_small.empty:
    process_language(current_language, current_large, current_small, current_large_list, output_path)

# Resultado global ============================================================================================
final_results: list[pd.DataFrame] = []
if not large_files_commits.empty:
    final_results.append(together_change(large_files_commits, large_files_list_geral))
if not small_files_commits.empty:
    final_results.append(together_change(small_files_commits, large_files_list_geral, 'small'))

if final_results:
    pd.concat(final_results).to_csv(f"{output_path}/global_results.csv", index=False)