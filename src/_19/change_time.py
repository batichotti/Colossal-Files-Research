import pandas as pd
from os import makedirs, path
from sys import setrecursionlimit
import numpy as np
import datetime

setrecursionlimit(2_000_000)

SEPARATOR = '|'

# Setup =======================================================================================================
input_path: str = "./src/_19/input/"
output_path: str = "./src/_19/output/"

repositories_path: str = "./src/_00/input/450_Starred_Projects.csv"
language_white_list_path: str = "./src/_15/input/white_list.csv"
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

# Funções auxiliares =========================================================================================
def grew_or_decreased(repository_commits: pd.DataFrame, change_type: str = "large") -> pd.DataFrame:
    """Verifica como foi o crescimento e a diminuição dos arquivos"""

    added_files: pd.DataFrame = repository_commits[repository_commits['Change Type'] == 'ADD'].copy()
    added_files_total: int = len(added_files)

    if not added_files.empty:
        added_files = added_files[added_files['File Name'].apply(lambda x: isinstance(x, str) and '.' in x)]
        if not added_files.empty:
            added_files['Extension'] = added_files['File Name'].apply(lambda x: x.split(".")[-1]).copy()
            added_files = added_files[added_files['Extension'].isin(language_white_list_df['Extension'].values)]
            added_files = added_files.merge(
                language_white_list_df[['Extension', 'Language']],
                on='Extension',
                how='left'
            ).drop(columns=['Extension'])
    added_files_filtered_total:int = len(added_files)

    changes = repository_commits[repository_commits['Local File PATH New'].isin(added_files['Local File PATH New'].values)].copy()
    changes = changes.merge(added_files[['Local File PATH New', 'Language']], on='Local File PATH New', how='left')

    changes_large: pd.DataFrame = changes.copy()
    if not changes_large.empty:
        # Converte NLOC para numérico e remove inválidos
        changes_large['Lines Of Code (nloc)'] = pd.to_numeric(changes_large['Lines Of Code (nloc)'], errors='coerce')
        changes_large = changes_large.dropna(subset=['Language', 'Lines Of Code (nloc)'])

        # Filtra as linhas onde a linguagem é igual e o número de linhas de código é menor que o percentil 99
        percentil_99 = percentil_df.set_index('language')['percentil 99']
        changes_large = changes_large[changes_large.apply(
            lambda x: x['Lines Of Code (nloc)'] >= percentil_99.get(x['Language'], 0), 
            axis=1
        )]

    changes_small = pd.DataFrame()
    if not changes_large.empty:
        changes_small = changes[~changes['Local File PATH New'].isin(changes_large['Local File PATH New'].values)].copy()
        changes_large = changes[changes['Local File PATH New'].isin(changes_large['Local File PATH New'].values)].copy()


    # ANAL. ============================================================================================================
    changes_files_total: int = 0
    change_time_total = []
    only_added_total: int = 0
    if not changes.empty:
        changes_files_total = len(changes.groupby('Local File PATH New'))
        changes['Committer Commit Date'] = changes['Committer Commit Date'].apply(
            lambda x: x[:-3] + x[-2:]  # Remove o ':' do offset (+02:00 → +0200)
        )
        changes['Committer Commit Date'] = changes['Committer Commit Date'].apply(
            lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S%z').astimezone(datetime.timezone.utc)
        )
        changes = changes.sort_values(by='Committer Commit Date')
        
        for _, file_changes in changes.groupby('Local File PATH New'):
            commits = file_changes['Committer Commit Date'].tolist()
            if len(commits) >= 2:
                for i in range(1, len(commits)):
                    delta = (commits[i] - commits[i-1]).total_seconds()
                    change_time_total.append(delta)
            else:
                only_added_total += 1

    changes_large_files_total: int = 0
    change_time_large_total = []
    only_added_large_total: int = 0
    if not changes_large.empty:
        changes_large_files_total = len(changes_large.groupby('Local File PATH New'))
        changes_large['Committer Commit Date'] = changes_large['Committer Commit Date'].apply(
            lambda x: x[:-3] + x[-2:]
        )
        changes_large['Committer Commit Date'] = changes_large['Committer Commit Date'].apply(
            lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S%z').astimezone(datetime.timezone.utc)
        )
        changes_large = changes_large.sort_values(by='Committer Commit Date')
        for _, file_changes in changes_large.groupby('Local File PATH New'):
            commits = file_changes['Committer Commit Date'].tolist()
            if len(commits) >= 2:
                for i in range(1, len(commits)):
                    delta = (commits[i] - commits[i-1]).total_seconds()
                    change_time_large_total.append(delta)
            else:
                only_added_large_total += 1

    changes_small_files_total: int = 0
    change_time_small_total = []
    only_added_small_total: int = 0
    if not changes_small.empty:
        changes_small_files_total = len(changes_small.groupby('Local File PATH New'))
        changes_small['Committer Commit Date'] = changes_small['Committer Commit Date'].apply(
            lambda x: x[:-3] + x[-2:]  # Aplicar o mesmo ajuste
        )
        changes_small['Committer Commit Date'] = changes_small['Committer Commit Date'].apply(
            lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S%z').astimezone(datetime.timezone.utc)
        )
        changes_small = changes_small.sort_values(by='Committer Commit Date')
        for _, file_changes in changes_small.groupby('Local File PATH New'):
            commits = file_changes['Committer Commit Date'].tolist()
            if len(commits) >= 2:
                for i in range(1, len(commits)):
                    delta = (commits[i] - commits[i-1]).total_seconds()
                    change_time_small_total.append(delta)
            else:
                only_added_small_total += 1
    
    # Result =================================================================================================================
    result: dict = {
        "Type": [change_type],
        "#Files": [added_files_total],
        "#Files Filtered by Language": [added_files_filtered_total],

        "Total Files": [changes_files_total],
        "Only Added": [only_added_total],
        "Time Average": [np.mean(change_time_total) if change_time_total else 0],
        "Time Median": [np.median(change_time_total) if change_time_total else 0],
        
        "Total Large Files": [changes_large_files_total],
        "Only Added Large": [only_added_large_total],
        "Time Large Average": [np.mean(change_time_large_total) if change_time_large_total else 0],
        "Time Large Median": [np.median(change_time_large_total) if change_time_large_total else 0],

        "Total Small Files": [changes_small_files_total],
        "Only Added Small": [only_added_small_total],
        "Time Small Average": [np.mean(change_time_small_total) if change_time_small_total else 0],
        "Time Small Median": [np.median(change_time_small_total) if change_time_small_total else 0],
        
        "Large p/ Small (Average)": [
            (np.mean(change_time_small_total) / np.mean(change_time_large_total)
            if change_time_small_total and change_time_large_total
            else 0)
        ],

        "Large p/ Small (Median)": [
            (np.median(change_time_small_total) / np.median(change_time_large_total)
            if change_time_small_total and change_time_large_total
            else 0)
        ]
    }
    return pd.DataFrame(result)
'''
/home/usuario01/Colossal-Files-Research/src/_19/change_time.py:166: RuntimeWarning: invalid value encountered in scalar divide
(np.median(change_time_small_total) / np.median(change_time_large_total)
'''

def process_language(lang: str, large: pd.DataFrame, small: pd.DataFrame, output_path: str):
    """Processa e salva resultados por linguagem"""
    results:list[pd.DataFrame] = []
    if not large.empty:
        results.append(grew_or_decreased(large, 'large'))
    if not small.empty:
        results.append(grew_or_decreased(small, 'small'))
    
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

    print(repo_path)

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
        project_results.append(grew_or_decreased(large_df))
    if not small_df.empty:
        project_results.append(grew_or_decreased(small_df, 'small'))

    if project_results:
        pd.concat(project_results).to_csv(f"{output_path}/per_project/{repo_path}.csv", index=False)

# Processa última linguagem
if not current_large.empty or not current_small.empty:
    process_language(current_language, current_large, current_small, output_path)

# Resultado global ============================================================================================
final_results: list[pd.DataFrame] = []
if not large_files_commits.empty:
    final_results.append(grew_or_decreased(large_files_commits))
if not small_files_commits.empty:
    final_results.append(grew_or_decreased(small_files_commits, 'small'))

if final_results:
    pd.concat(final_results).to_csv(f"{output_path}/global_results.csv", index=False)
