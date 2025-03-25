import pandas as pd
from os import makedirs, path
from sys import setrecursionlimit
import numpy as np
import datetime
from scipy.stats import pearsonr, spearmanr


setrecursionlimit(2_000_000)

SEPARATOR = '|'

# Setup =======================================================================================================
input_path: str = "./src/_22/input/"
output_path: str = "./src/_22/output/"

repositories_path: str = "./src/_00/input/450_Starred_Projects.csv"
language_white_list_path: str = "./src/_12/input/white_list.csv"
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
def correlations(repository_commits: pd.DataFrame, change_type: str = "large") -> pd.DataFrame:
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
    lifetime_total = []
    freq_total = []
    deleted_amount_total = []
    deleted_lifetime_total = []
    deleted_freq_total = []
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
            amount = len(commits)
            if amount >= 2:
                delta = (commits[-1] - commits[0]).total_seconds()
                change_amount_total.append(amount)
                lifetime_total.append(delta)
                freq_total.append(delta/amount)
            else:
                only_added_total += 1
            if "DELETE" in file_changes['Change Type'].values:
                deleted_total += 1
                deleted_amount_total.append(amount)
                deleted_lifetime_total.append(delta)
                deleted_freq_total.append(delta/amount)

    change_amount_large_total = []
    lifetime_large_total = []
    freq_large_total = []
    deleted_amount_large_total = []
    deleted_lifetime_large_total = []
    deleted_freq_large_total = []
    only_added_large_total: int = 0
    deleted_large_total: int = 0
    if not changes_large.empty:
        changes_large['File Path'] = changes_large.apply(
            lambda x: x['Local File PATH New'] if pd.notna(x['Local File PATH New']) 
                    else x['Local File PATH Old'], 
            axis=1
        )

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
            amount = len(commits)
            if amount >= 2:
                delta = (commits[-1] - commits[0]).total_seconds()
                change_amount_large_total.append(amount)
                lifetime_large_total.append(delta)
                freq_large_total.append(delta/amount)
            else:
                only_added_large_total += 1
            if "DELETE" in file_changes['Change Type'].values:
                deleted_large_total += 1
                deleted_amount_large_total.append(amount)
                deleted_lifetime_large_total.append(delta)
                deleted_freq_large_total.append(delta/amount)

    change_amount_small_total = []
    lifetime_small_total = []
    freq_small_total = []
    deleted_amount_small_total = []
    deleted_lifetime_small_total = []
    deleted_freq_small_total = []
    only_added_small_total: int = 0
    deleted_small_total: int = 0
    if not changes_small.empty:
        changes_small['File Path'] = changes_small.apply(
            lambda x: x['Local File PATH New'] if pd.notna(x['Local File PATH New']) 
                    else x['Local File PATH Old'], 
            axis=1
        )

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
            amount = len(commits)
            if amount >= 2:
                delta = (commits[-1] - commits[0]).total_seconds()
                change_amount_small_total.append(amount)
                lifetime_small_total.append(delta)
                freq_small_total.append(delta/amount)
            else:
                only_added_small_total += 1
            if "DELETE" in file_changes['Change Type'].values:
                deleted_small_total += 1
                deleted_amount_small_total.append(amount)
                deleted_lifetime_small_total.append(delta)
                deleted_freq_small_total.append(delta/amount)

    # Cálculo de Correlações ==============================================================================================
    def compute_correlations(amounts, lifetimes):
        if len(amounts) > 1 and len(lifetimes) > 1:
            # Check if either array has zero variance
            if np.std(amounts) == 0 or np.std(lifetimes) == 0:
                return (np.nan, np.nan)
            else:
                pearson_corr, _ = pearsonr(amounts, lifetimes)
                spearman_corr, _ = spearmanr(amounts, lifetimes)
                return (pearson_corr, spearman_corr)
        else:
            return (0, 0)

    pearson_corr_geral, spearman_corr_geral = compute_correlations(change_amount_total, lifetime_total)
    pearson_corr_large, spearman_corr_large = compute_correlations(change_amount_large_total, lifetime_large_total)
    pearson_corr_small, spearman_corr_small = compute_correlations(change_amount_small_total, lifetime_small_total)

    result: dict = {
        "Type": [change_type],
        "#Files": [added_files_total],

        # Geral
        "Total Filtered Files": [changes_files_total],

        # Correlações
        "Pearson Correlation (Geral)": [pearson_corr_geral],
        "Spearman Correlation (Geral)": [spearman_corr_geral],
        "Pearson Correlation (Large)": [pearson_corr_large],
        "Spearman Correlation (Large)": [spearman_corr_large],
        "Pearson Correlation (Small)": [pearson_corr_small],
        "Spearman Correlation (Small)": [spearman_corr_small]
    }
    return pd.DataFrame(result)

def process_language(lang: str, large: pd.DataFrame, small: pd.DataFrame, output_path: str):
    """Processa e salva resultados por linguagem"""
    results:list[pd.DataFrame] = []
    if not large.empty:
        results.append(correlations(large, 'large'))
    if not small.empty:
        results.append(correlations(small, 'small'))
    
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
        project_results.append(correlations(large_df))
    if not small_df.empty:
        project_results.append(correlations(small_df, 'small'))

    if project_results:
        pd.concat(project_results).to_csv(f"{output_path}/per_project/{repo_path}.csv", index=False)

# Processa última linguagem
if not current_large.empty or not current_small.empty:
    process_language(current_language, current_large, current_small, output_path)

# Resultado global ============================================================================================
final_results: list[pd.DataFrame] = []
if not large_files_commits.empty:
    final_results.append(correlations(large_files_commits))
if not small_files_commits.empty:
    final_results.append(correlations(small_files_commits, 'small'))

if final_results:
    pd.concat(final_results).to_csv(f"{output_path}/global_results.csv", index=False)
