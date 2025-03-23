import pandas as pd
from os import makedirs, path
from sys import setrecursionlimit
import numpy as np
import datetime

setrecursionlimit(2_000_000)

SEPARATOR = '|'

# Setup =======================================================================================================
input_path: str = "./src/_21/input/"
output_path: str = "./src/_21/output/"

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
def change_time(repository_commits: pd.DataFrame, change_type: str = "large") -> pd.DataFrame:
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
    balances_grow = []
    balances_decreased = []
    balances_zero = []
    only_added_total = 0

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
            file_changes = file_changes.sort_values(by='Committer Commit Date')
            file_changes['Lines Of Code (nloc)'] = pd.to_numeric(file_changes['Lines Of Code (nloc)'], errors='coerce')
            file_changes = file_changes.dropna(subset=['Lines Of Code (nloc)'])
            if not file_changes.empty:
                nlocs = file_changes['Lines Of Code (nloc)'].tolist()
                size = len(nlocs)
                if "DELETE" not in file_changes['Change Type'].values:
                    size = len(nlocs)-1
                if (size > 1):
                    for i in range(0, size-2):
                        balance = nlocs[i+1] - nlocs[i]
                        if balance > 0:
                            balances_grow.append(balance)
                        elif balance < 0:
                            balances_decreased.append(balance)
                        else:
                            balances_zero.append(balance)
                else:
                    only_added_total += 1

    changes_large_files_total: int = 0
    balances_large_grow = []
    balances_large_decreased = []
    balances_large_zero = []
    only_added_large_total: int = 0

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
        
        for _, file_changes in changes.groupby('File Path'):
            file_changes = file_changes.sort_values(by='Committer Commit Date')
            file_changes['Lines Of Code (nloc)'] = pd.to_numeric(file_changes['Lines Of Code (nloc)'], errors='coerce')
            file_changes = file_changes.dropna(subset=['Lines Of Code (nloc)'])
            if not file_changes.empty:
                nlocs = file_changes['Lines Of Code (nloc)'].tolist()
                size = len(nlocs)
                if "DELETE" not in file_changes['Change Type'].values:
                    size = len(nlocs)-1
                if (size > 1):
                    for i in range(0, size-2):
                        balance = nlocs[i+1] - nlocs[i]
                        if balance > 0:
                            balances_large_grow.append(balance)
                        elif balance < 0:
                            balances_large_decreased.append(balance)
                        else:
                            balances_large_zero.append(balance)
                else:
                    only_added_large_total += 1

    changes_small_files_total: int = 0
    balances_small_grow = []
    balances_small_decreased = []
    balances_small_zero = []
    only_added_small_total: int = 0
    if not changes_small.empty:
        changes_small['File Path'] = changes_small.apply(
            lambda x: x['Local File PATH New'] if pd.notna(x['Local File PATH New']) 
                    else x['Local File PATH Old'], 
            axis=1
        )
        changes_small_files_total = len(changes_small.groupby('File Path'))

        changes_small['Committer Commit Date'] = changes_small['Committer Commit Date'].apply(
            lambda x: x[:-3] + x[-2:]  # Aplicar o mesmo ajuste
        )
        changes_small['Committer Commit Date'] = changes_small['Committer Commit Date'].apply(
            lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S%z').astimezone(datetime.timezone.utc)
        )
        changes_small = changes_small.sort_values(by='Committer Commit Date')
        
        for _, file_changes in changes.groupby('File Path'):
            file_changes = file_changes.sort_values(by='Committer Commit Date')
            file_changes['Lines Of Code (nloc)'] = pd.to_numeric(file_changes['Lines Of Code (nloc)'], errors='coerce')
            file_changes = file_changes.dropna(subset=['Lines Of Code (nloc)'])
            if not file_changes.empty:
                nlocs = file_changes['Lines Of Code (nloc)'].tolist()
                size = len(nlocs)
                if "DELETE" not in file_changes['Change Type'].values:
                    size = len(nlocs)-1
                if (size > 1):
                    for i in range(0, size-2):
                        balance = nlocs[i+1] - nlocs[i]
                        if balance > 0:
                            balances_small_grow.append(balance)
                        elif balance < 0:
                            balances_small_decreased.append(balance)
                        else:
                            balances_small_zero.append(balance)
                else:
                    only_added_small_total += 1
    
    # Compute averages and medians for small and large changes
    avg_grew_geral = np.mean(balances_grow) if balances_grow else 0
    med_grew_geral = np.median(balances_grow) if balances_grow else 0
    avg_decreased_geral = np.mean(balances_decreased) if balances_decreased else 0
    med_decreased_geral = np.median(balances_decreased) if balances_decreased else 0
    grew_percentage = ((len(balances_grow)/changes_files_total)*100) if changes_files_total != 0 else 0
    decreased_percentage = ((len(balances_decreased)/changes_files_total)*100) if changes_files_total != 0 else 0
    zero_percentage = ((len(balances_zero)/changes_files_total)*100) if changes_files_total != 0 else 0

    avg_large_grew_geral = np.mean(balances_large_grow) if balances_large_grow else 0
    med_large_grew_geral = np.median(balances_large_grow) if balances_large_grow else 0
    avg_large_decreased_geral = np.mean(balances_large_decreased) if balances_large_decreased else 0
    med_large_decreased_geral = np.median(balances_large_decreased) if balances_large_decreased else 0
    grew_large_percentage = ((len(balances_large_grow)/changes_large_files_total)*100) if changes_large_files_total != 0 else 0
    decreased_large_percentage = ((len(balances_large_decreased)/changes_large_files_total)*100) if changes_large_files_total != 0 else 0
    zero_large_percentage = ((len(balances_large_zero)/changes_large_files_total)*100) if changes_large_files_total != 0 else 0

    avg_small_grew_geral = np.mean(balances_small_grow) if balances_small_grow else 0
    med_small_grew_geral = np.median(balances_small_grow) if balances_small_grow else 0
    avg_small_decreased_geral = np.mean(balances_small_decreased) if balances_small_decreased else 0
    med_small_decreased_geral = np.median(balances_small_decreased) if balances_small_decreased else 0
    grew_small_percentage = ((len(balances_small_grow)/changes_small_files_total)*100) if changes_small_files_total != 0 else 0
    decreased_small_percentage = ((len(balances_small_decreased)/changes_small_files_total)*100) if changes_small_files_total != 0 else 0
    zero_small_percentage = ((len(balances_small_zero)/changes_small_files_total)*100) if changes_small_files_total != 0 else 0

    # Result ===========================================================================================================
    result: dict = {
        "Type": [change_type],
        "#Files": [added_files_total],

        "Total Filtered Files": [changes_files_total],
        "%% Grew": [grew_percentage],
        "%% Decreased": [decreased_percentage],
        "%% Zero": [zero_percentage],
        "Grew Average": [avg_grew_geral],
        "Grew Median": [med_grew_geral],
        "Decreased Average": [avg_decreased_geral],
        "Decreased Median": [med_decreased_geral],
        "Zero Total": [len(balances_zero)],

        "Total Large Files": [changes_large_files_total],
        "%% Large Grew": [grew_large_percentage],
        "%% Large Decreased": [decreased_large_percentage],
        "%% Large Zero": [zero_large_percentage],
        "Large Grew Average": [avg_large_grew_geral],
        "Large Grew Median": [med_large_grew_geral],
        "Large Decreased Average": [avg_large_decreased_geral],
        "Large Decreased Median": [med_large_decreased_geral],
        "Large Zero Total": [len(balances_large_zero)],

        "Total Small Files": [changes_small_files_total],
        "%% Small Grew": [grew_small_percentage],
        "%% Small Decreased": [decreased_small_percentage],
        "%% Small Zero": [zero_small_percentage],
        "Small Grew Average": [avg_small_grew_geral],
        "Small Grew Median": [med_small_grew_geral],
        "Small Decreased Average": [avg_small_decreased_geral],
        "Small Decreased Median": [med_small_decreased_geral],
        "Small Zero Total": [len(balances_small_zero)]
    }
    return pd.DataFrame(result)

def process_language(lang: str, large: pd.DataFrame, small: pd.DataFrame, output_path: str):
    """Processa e salva resultados por linguagem"""
    results:list[pd.DataFrame] = []
    if not large.empty:
        results.append(change_time(large, 'large'))
    if not small.empty:
        results.append(change_time(small, 'small'))
    
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
        project_results.append(change_time(large_df))
    if not small_df.empty:
        project_results.append(change_time(small_df, 'small'))

    if project_results:
        pd.concat(project_results).to_csv(f"{output_path}/per_project/{repo_path}.csv", index=False)

# Processa última linguagem
if not current_large.empty or not current_small.empty:
    process_language(current_language, current_large, current_small, output_path)

# Resultado global ============================================================================================
final_results: list[pd.DataFrame] = []
if not large_files_commits.empty:
    final_results.append(change_time(large_files_commits))
if not small_files_commits.empty:
    final_results.append(change_time(small_files_commits, 'small'))

if final_results:
    pd.concat(final_results).to_csv(f"{output_path}/global_results.csv", index=False)
