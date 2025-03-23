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
def frequency_by_lifetime(repository_commits: pd.DataFrame, change_type: str = "large") -> pd.DataFrame:
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

    changes = repository_commits[
        repository_commits['Local File PATH New'].isin(added_files['Local File PATH New'].values) |
        repository_commits['Local File PATH New'].isin(added_files['Local File PATH Old'].values)
        ].copy()

    # Cria um mapeamento completo de TODOS os caminhos (New e Old) para linguagem
    path_to_language = pd.concat([
        added_files[['Local File PATH New', 'Language']].rename(columns={'Local File PATH New': 'Path'}),
        added_files[['Local File PATH Old', 'Language']].rename(columns={'Local File PATH Old': 'Path'})
    ]).dropna(subset=['Path']).set_index('Path')['Language'].to_dict()
    # Atribui a linguagem baseada em ambos os caminhos
    changes['Language'] = changes.apply(
        lambda x: (
            path_to_language.get(x['Local File PATH New']) or 
            path_to_language.get(x['Local File PATH Old'])
        ),
        axis=1
    )
    added_files_filtered_total:int = len(added_files)

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
        large_paths = pd.concat([
            changes_large['Local File PATH New'],
            changes_large['Local File PATH Old']
        ])
        changes_small = changes[~changes['Local File PATH New'].isin(large_paths) &
                                ~changes['Local File PATH Old'].isin(large_paths)
                                ].copy()
        changes_large = changes[changes['Local File PATH New'].isin(large_paths) |
                                changes['Local File PATH Old'].isin(large_paths)
                                ].copy()


    # ANAL. ============================================================================================================
    changes_files_total: int = 0
    change_amount_total = []
    deleted_amount_total = []
    only_added_total: int = 0
    deleted_total: int = 0
    if not changes.empty:
        # Cria uma chave de agrupamento combinando New e Old paths
        changes['File Path'] = changes.apply(
            lambda x: x['Local File PATH New'] if pd.notna(x['Local File PATH New']) 
                    else x['Local File PATH Old'], 
            axis=1
        )
        changes_files_total = len(changes.groupby('File Path'))

        changes['Committer Commit Date'] = changes['Committer Commit Date'].apply(
            lambda x: x[:-3] + x[-2:]  # Remove o ':' do offset (+02:00 → +0200)
        )
        changes['Committer Commit Date'] = changes['Committer Commit Date'].apply(
            lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S%z').astimezone(datetime.timezone.utc)
        )
        changes = changes.sort_values(by='Committer Commit Date')
        
        for _, file_changes in changes.groupby('File Path'):
            commits = file_changes['Committer Commit Date'].tolist()
            delta = 0
            if len(commits) >= 2:
                delta = (commits[0] - commits[-1]).total_seconds()
                change_amount_total.append(delta)
            else:
                only_added_total += 1
            if "DELETE" in file_changes['Change Type'].values:
                deleted_amount_total.append(delta)
                deleted_total += 1

    changes_large_files_total: int = 0
    change_amount_large_total = []
    deleted_amount_large_total = []
    only_added_large_total: int = 0
    deleted_large_total: int = 0
    if not changes_large.empty:
        changes_large['File Path'] = changes_large.apply(
            lambda x: x['Local File PATH New'] if pd.notna(x['Local File PATH New']) 
                    else x['Local File PATH Old'], 
            axis=1
        )
        changes_large_files_total = len(changes_large.groupby('File Path'))

        changes_large['Committer Commit Date'] = changes_large['Committer Commit Date'].apply(
            lambda x: x[:-3] + x[-2:]
        )
        changes_large['Committer Commit Date'] = changes_large['Committer Commit Date'].apply(
            lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S%z').astimezone(datetime.timezone.utc)
        )
        changes_large = changes_large.sort_values(by='Committer Commit Date')
        
        for _, file_changes in changes_large.groupby('File Path'):
            commits = file_changes['Committer Commit Date'].tolist()
            delta = 0
            if len(commits) >= 2:
                delta = (commits[0] - commits[-1]).total_seconds()
                change_amount_large_total.append(delta)
            else:
                only_added_large_total += 1
            if "DELETE" in file_changes['Change Type'].values:
                deleted_amount_large_total.append(delta)
                deleted_large_total += 1

    changes_small_files_total: int = 0
    change_amount_small_total = []
    deleted_amount_small_total = []
    only_added_small_total: int = 0
    deleted_small_total: int = 0
    if not changes_small.empty:
        changes_small['File Path'] = changes_small.apply(
            lambda x: x['Local File PATH New'] if pd.notna(x['Local File PATH New']) 
                    else x['Local File PATH Old'], 
            axis=1
        )
        changes_small_files_total = len(changes_small.groupby('File Path'))

        changes_small['Committer Commit Date'] = changes_small['Committer Commit Date'].apply(
            lambda x: x[:-3] + x[-2:]
        )
        changes_small['Committer Commit Date'] = changes_small['Committer Commit Date'].apply(
            lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S%z').astimezone(datetime.timezone.utc)
        )
        changes_small = changes_small.sort_values(by='Committer Commit Date')
        
        for _, file_changes in changes_small.groupby('File Path'):
            commits = file_changes['Committer Commit Date'].tolist()
            delta = 0
            if len(commits) >= 2:
                delta = (commits[0] - commits[-1]).total_seconds()
                change_amount_small_total.append(delta)
            else:
                only_added_small_total += 1
            if "DELETE" in file_changes['Change Type'].values:
                deleted_amount_small_total.append(delta)
                deleted_small_total += 1
    
    # Compute averages and medians for small and large changes
    avg_geral = np.mean(change_amount_total) if change_amount_total else 0
    med_geral = np.median(change_amount_total) if change_amount_total else 0
    del_avg_geral = np.mean(deleted_amount_total) if deleted_amount_total else 0
    del_med_geral = np.median(deleted_amount_total) if deleted_amount_total else 0

    avg_large = np.mean(change_amount_large_total) if change_amount_large_total else 0
    med_large = np.median(change_amount_large_total) if change_amount_large_total else 0
    del_avg_large = np.mean(deleted_amount_large_total) if deleted_amount_large_total else 0
    del_med_large = np.median(deleted_amount_large_total) if deleted_amount_large_total else 0

    avg_small = np.mean(change_amount_small_total) if change_amount_small_total else 0
    med_small = np.median(change_amount_small_total) if change_amount_small_total else 0
    del_avg_small = np.mean(deleted_amount_small_total) if deleted_amount_small_total else 0
    del_med_small = np.median(deleted_amount_small_total) if deleted_amount_small_total else 0

    # Calculate ratios with checks for division by zero
    ratio_avg = (avg_small / avg_large) if avg_large != 0 else 0
    ratio_med = (med_small / med_large) if med_large != 0 else 0
    ratio_del_avg = (del_avg_small / del_avg_large) if del_avg_large != 0 else 0
    ratio_del_med = (del_med_small / del_med_large) if del_med_large != 0 else 0

    # Result ===========================================================================================================
    result: dict = {
        "Type": [change_type],
        "#Files": [added_files_total],
        "#Files Filtered by Language": [added_files_filtered_total],

        "Total Files": [changes_files_total],
        "Only Added": [only_added_total],
        "#Commits Average": [avg_geral],
        "#Commits Median": [med_geral],
        "Deleted #Commits Average": [del_avg_geral],
        "Deleted #Commits Median": [del_med_geral],
        
        "Total Large Files": [changes_large_files_total],
        "Only Added Large": [only_added_large_total],
        "#Commits Large Average": [avg_large],
        "#Commits Large Median": [med_large],
        "Deleted #Commits Large Average": [del_avg_large],
        "Deleted #Commits Large Median": [del_med_large],

        "Total Small Files": [changes_small_files_total],
        "Only Added Small": [only_added_small_total],
        "#Commits Small Average": [avg_small],
        "#Commits Small Median": [med_small],
        "Deleted #Commits Small Average": [del_avg_small],
        "Deleted #Commits Small Median": [del_med_small],
        
        "Large p/ Small (Average)": [ratio_avg],
        "Large p/ Small (Median)": [ratio_med],
        "Deleted Large p/ Small (Average)": [ratio_del_avg],
        "Deleted Large p/ Small (Median)": [ratio_del_med]
    }
    return pd.DataFrame(result)

def process_language(lang: str, large: pd.DataFrame, small: pd.DataFrame, output_path: str):
    """Processa e salva resultados por linguagem"""
    results:list[pd.DataFrame] = []
    if not large.empty:
        results.append(frequency_by_lifetime(large, 'large'))
    if not small.empty:
        results.append(frequency_by_lifetime(small, 'small'))
    
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
        project_results.append(frequency_by_lifetime(large_df))
    if not small_df.empty:
        project_results.append(frequency_by_lifetime(small_df, 'small'))

    if project_results:
        pd.concat(project_results).to_csv(f"{output_path}/per_project/{repo_path}.csv", index=False)

# Processa última linguagem
if not current_large.empty or not current_small.empty:
    process_language(current_language, current_large, current_small, output_path)

# Resultado global ============================================================================================
final_results: list[pd.DataFrame] = []
if not large_files_commits.empty:
    final_results.append(frequency_by_lifetime(large_files_commits))
if not small_files_commits.empty:
    final_results.append(frequency_by_lifetime(small_files_commits, 'small'))

if final_results:
    pd.concat(final_results).to_csv(f"{output_path}/global_results.csv", index=False)
