import pandas as pd
from os import makedirs, path
from sys import setrecursionlimit
import numpy as np

setrecursionlimit(2_000_000)

SEPARATOR = '|'

# Setup =======================================================================================================
input_path: str = "./src/_18/input/"
output_path: str = "./src/_18/output/"

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
    balances_grow: list[int] = []
    balances_decreased: list[int] = []
    balances_zero: list[int] = []
    deleted_total: int = 0
    if not changes.empty:
        # Cria uma chave de agrupamento combinando New e Old paths
        changes['File Path'] = changes.apply(
            lambda x: x['Local File PATH New'] if pd.notna(x['Local File PATH New']) 
                    else x['Local File PATH Old'], 
            axis=1
        )
        changes_files_total = len(changes.groupby('File Path'))

        files_deleted = changes[changes['Change Type'] == 'DELETE']
        deleted_total = len(files_deleted)
        changes = changes[~changes['File Path'].isin(files_deleted['File Path'].values)]
        if not changes.empty:
            changes_per_file = changes.groupby('File Path')
            for _, file_changes in changes_per_file:
                file_changes = file_changes.sort_values(by='Committer Commit Date')
                file_changes['Lines Of Code (nloc)'] = pd.to_numeric(file_changes['Lines Of Code (nloc)'], errors='coerce')
                file_changes = file_changes.dropna(subset=['Lines Of Code (nloc)'])
                if not file_changes.empty:
                    first_nloc = file_changes['Lines Of Code (nloc)'].iloc[0]
                    last_nloc = file_changes['Lines Of Code (nloc)'].iloc[-1]
                    balance = last_nloc - first_nloc
                    if balance > 0:
                        balances_grow.append(balance)
                    elif balance < 0:
                        balances_decreased.append(balance)
                    else:
                        balances_zero.append(balance)

    changes_large_files_total: int = 0
    balances_large_grow: list[int] = []
    balances_large_decreased: list[int] = []
    balances_large_zero: list[int] = []
    deleted_large_total: int = 0
    if not changes_large.empty:
        # Cria uma chave de agrupamento combinando New e Old paths
        changes_large['File Path'] = changes_large.apply(
            lambda x: x['Local File PATH New'] if pd.notna(x['Local File PATH New']) 
                    else x['Local File PATH Old'], 
            axis=1
        )
        changes_large_files_total = len(changes_large.groupby('File Path'))

        files_deleted_large = changes_large[changes_large['Change Type'] == 'DELETE']
        deleted_large_total = len(files_deleted_large)
        changes_large = changes_large[~changes_large['File Path'].isin(files_deleted_large['File Path'].values)]
        if not changes_large.empty:
            changes_large = changes_large.sort_values(by='Committer Commit Date')
            changes_large_per_file = changes_large.groupby('File Path')
            for _, file_changes in changes_large_per_file:
                file_changes = file_changes.sort_values(by='Committer Commit Date')
                file_changes['Lines Of Code (nloc)'] = pd.to_numeric(file_changes['Lines Of Code (nloc)'], errors='coerce')
                file_changes = file_changes.dropna(subset=['Lines Of Code (nloc)'])
                if not file_changes.empty:
                    first_nloc = file_changes['Lines Of Code (nloc)'].iloc[0]
                    last_nloc = file_changes['Lines Of Code (nloc)'].iloc[-1]
                    balance = last_nloc - first_nloc
                    if balance > 0:
                        balances_large_grow.append(balance)
                    elif balance < 0:
                        balances_large_decreased.append(balance)
                    else:
                        balances_large_zero.append(balance)

    changes_small_files_total: int = 0
    balances_small_grow: list[int] = []
    balances_small_decreased: list[int] = []
    balances_small_zero: list[int] = []
    deleted_small_total: int = 0
    if not changes_small.empty:
        # Cria uma chave de agrupamento combinando New e Old paths
        changes_small['File Path'] = changes_small.apply(
            lambda x: x['Local File PATH New'] if pd.notna(x['Local File PATH New']) 
                    else x['Local File PATH Old'], 
            axis=1
        )
        changes_small_files_total = len(changes_small.groupby('File Path'))

        files_deleted_small = changes_small[changes_small['Change Type'] == 'DELETE']
        deleted_small_total = len(files_deleted_small)
        changes_small = changes_small[~changes_small['File Path'].isin(files_deleted_small['File Path'].values)]
        if not changes_small.empty:
            changes_small = changes_small.sort_values(by='Committer Commit Date')
            changes_small_per_file = changes_small.groupby('File Path')
            for _, file_changes in changes_small_per_file:
                file_changes = file_changes.sort_values(by='Committer Commit Date')
                file_changes['Lines Of Code (nloc)'] = pd.to_numeric(file_changes['Lines Of Code (nloc)'], errors='coerce')
                file_changes = file_changes.dropna(subset=['Lines Of Code (nloc)'])
                if not file_changes.empty:
                    first_nloc = file_changes['Lines Of Code (nloc)'].iloc[0]
                    last_nloc = file_changes['Lines Of Code (nloc)'].iloc[-1]
                    balance = last_nloc - first_nloc
                    if balance > 0:
                        balances_small_grow.append(balance)
                    elif balance < 0:
                        balances_small_decreased.append(balance)
                    else:
                        balances_small_zero.append(balance)
    
    # Result =================================================================================================================
    result: dict = {
        "Type": [change_type],
        "#Files": [added_files_total],
        "#Files Filtered by Language": [changes_files_total],
        "%% Grow": [(len(balances_grow)/changes_files_total)*100 if changes_files_total > 0 else 0],
        "%% Decrease": [(len(balances_decreased)/changes_files_total)*100 if changes_files_total > 0 else 0],
        "%% Zero": [(len(balances_zero)/changes_files_total)*100 if changes_files_total > 0 else 0],
        "%% Deleted": [(deleted_total/changes_files_total)*100 if changes_files_total > 0 else 0],

        "Total File Grow": [len(balances_grow)],
        "Grow Average": [np.mean(balances_grow) if balances_grow else 0],
        "Grow Median": [np.median(balances_grow) if balances_grow else 0],
        "Grow Max": [np.max(balances_grow) if balances_grow else 0],
        "Total Decreased": [len(balances_decreased)],
        "Decreased Average": [np.mean(balances_decreased) if balances_decreased else 0],
        "Decreased Median": [np.median(balances_decreased) if balances_decreased else 0],
        "Decreased Max": [np.min(balances_decreased) if balances_decreased else 0],
        "Total Zero": [len(balances_zero)],
        "Total Deleted": [deleted_total],

        "Total Large Grow": [len(balances_large_grow)],
        "Grow Large Average": [np.mean(balances_large_grow) if balances_large_grow else 0],
        "Grow Large Median": [np.median(balances_large_grow) if balances_large_grow else 0],
        "Grow Large Max": [np.max(balances_large_grow) if balances_large_grow else 0],
        "Total Large Decreased": [len(balances_large_decreased)],
        "Decreased Large Average": [np.mean(balances_large_decreased) if balances_large_decreased else 0],
        "Decreased Large Median": [np.median(balances_large_decreased) if balances_large_decreased else 0],
        "Decreased Large Max": [np.min(balances_large_decreased) if balances_large_decreased else 0],
        "Total Large Zero": [len(balances_large_zero)],
        "Total Large Deleted": [deleted_large_total],

        "Total Small Grow": [len(balances_small_grow)],
        "Grow Small Average": [np.mean(balances_small_grow) if balances_small_grow else 0],
        "Grow Small Median": [np.median(balances_small_grow) if balances_small_grow else 0],
        "Grow Small Max": [np.max(balances_small_grow) if balances_small_grow else 0],
        "Total Small Decreased": [len(balances_small_decreased)],
        "Decreased Small Average": [np.mean(balances_small_decreased) if balances_small_decreased else 0],
        "Decreased Small Median": [np.median(balances_small_decreased) if balances_small_decreased else 0],
        "Decreased Small Max": [np.min(balances_small_decreased) if balances_small_decreased else 0],
        "Total Small Zero": [len(balances_small_zero)],
        "Total Small Deleted": [deleted_small_total],
    }
    return pd.DataFrame(result)

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