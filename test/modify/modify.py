import pandas as pd
from os import makedirs, path
from sys import setrecursionlimit
import numpy as np
import datetime

setrecursionlimit(2_000_000)

SEPARATOR = '|'

# Setup =======================================================================================================
input_path: str = "./test/modify/input/"
output_path: str = "./test/modify/output/"

repositories_path: str = "./src/_00/input/450_Starred_Projects.csv"
modify_path: str = "./test/modify/output/per_projct/"
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

    large_paths:pd.DataFrame = pd.DataFrame()
    if not changes_large.empty:
        large_paths = pd.concat([
            changes_large['Local File PATH New'],
            changes_large['Local File PATH Old']
        ])

    changes['File Category'] = changes.apply(
        lambda x: 'large' if x['Local File PATH New'] in large_paths.values or x['Local File PATH Old'] in large_paths.values else 'small',
        axis=1
    )

    # ANAL. ============================================================================================================
    big_dataframe:list[dict] = []

    if not changes.empty:
        # Cria uma chave de agrupamento combinando New e Old paths
        changes['File Path'] = changes.apply(
            lambda x: x['Local File PATH New'] if pd.notna(x['Local File PATH New'])
                    else x['Local File PATH Old'],
            axis=1
        )

        changes['Committer Commit Date'] = changes['Committer Commit Date'].apply(
            lambda x: x[:-3] + x[-2:]  # Remove o ':' do offset (+02:00 → +0200)
        )
        changes['Committer Commit Date'] = changes['Committer Commit Date'].apply(
            lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S%z').astimezone(datetime.timezone.utc)
        )
        changes = changes.sort_values(by='Committer Commit Date')

        for _, file_changes in changes.groupby('File Path'):
            commits = file_changes['Committer Commit Date'].tolist()
            only_added = False
            deleted = False
            delta = 0
            amount = len(commits)
            if amount >= 2:
                delta = (commits[-1] - commits[0]).total_seconds()
            else:
                only_added = True
            if "DELETE" in file_changes['Change Type'].values:
                deleted = True

            delta_days = delta / (60 * 60 * 24)
            interval = delta / amount if not only_added else 0
            interval_days = (delta_days / amount) / (60 * 60 * 24) if not only_added else 0

            big_dataframe.append(
                {
                    "Dataset": change_type,
                    "Project": file_changes["Project Name"].iloc[0],
                    "File Path": file_changes["File Path"].iloc[0],
                    "File Category": file_changes["File Category"].iloc[0],
                    "Only Added?": only_added,
                    "First Commit": commits[0],
                    "Last Commit": commits[-1],
                    "Deleted?": deleted,
                    "Modifications' Amount": amount,
                    "Life Time (second)": delta,
                    "Life Time (day)": delta_days,
                    "Modifications' Interval (second)": interval,
                    "Modifications' Interval (day)": interval_days,
                }
            )

    result = pd.DataFrame(big_dataframe)

    return pd.DataFrame(result)

def calc_metrics(df:pd.DataFrame) -> pd.DataFrame:
    groups = df.groupby("Dataset")
    dataset_large = groups.get_group("large") if "large" in groups.groups else pd.DataFrame()
    dataset_small = groups.get_group("small") if "small" in groups.groups else pd.DataFrame()
    large_commits = 0
    large_only_added = 0
    large_amount_mean = 0
    large_amount_median = 0
    large_life_time_mean = 0
    large_life_time_median = 0
    large_interval_mean = 0
    large_interval_median = 0
    large_deleted_total = 0
    large_deleted_amount_mean = 0
    large_deleted_amount_median = 0
    large_deleted_life_time_mean = 0
    large_deleted_life_time_median = 0
    large_deleted_interval_mean = 0
    large_deleted_interval_median = 0
    if not dataset_large.empty:
        large_commits = dataset_large["Modifications' Amount"].sum()
        large_only_added = len(dataset_large[dataset_large["Only Added?"] == True])
        large_amount_mean = dataset_large["Modifications' Amount"].mean()
        large_amount_median = dataset_large["Modifications' Amount"].median()
        large_life_time_mean = dataset_large["Life Time (day)"].mean()
        large_life_time_median = dataset_large["Life Time (day)"].median()
        large_interval_mean = dataset_large["Modifications' Interval (day)"].mean()
        large_interval_median = dataset_large["Modifications' Interval (day)"].median()
        large_deleted_total = len(dataset_large[dataset_large["Deleted?"] == True])
        large_deleted_amount_mean = dataset_large[dataset_large["Deleted?"] == True]["Modifications' Amount"].mean()
        large_deleted_amount_median = dataset_large[dataset_large["Deleted?"] == True]["Modifications' Amount"].median()
        large_deleted_life_time_mean = dataset_large[dataset_large["Deleted?"] == True]["Life Time (day)"].mean()
        large_deleted_life_time_median = dataset_large[dataset_large["Deleted?"] == True]["Life Time (day)"].median()
        large_deleted_interval_mean = dataset_large[dataset_large["Deleted?"] == True]["Modifications' Interval (day)"].mean()
        large_deleted_interval_median = dataset_large[dataset_large["Deleted?"] == True]["Modifications' Interval (day)"].median()

    small_commits = 0
    small_only_added = 0
    small_amount_mean = 0
    small_amount_median = 0
    small_life_time_mean = 0
    small_life_time_median = 0
    small_interval_mean = 0
    small_interval_median = 0
    small_deleted_total = 0
    small_deleted_amount_mean = 0
    small_deleted_amount_median = 0
    small_deleted_life_time_mean = 0
    small_deleted_life_time_median = 0
    small_deleted_interval_mean = 0
    small_deleted_interval_median = 0
    if not dataset_small.empty:
        small_commits = dataset_small["Modifications' Amount"].sum()
        small_only_added = len(dataset_small[dataset_small["Only Added?"] == True])
        small_amount_mean = dataset_small["Modifications' Amount"].mean()
        small_amount_median = dataset_small["Modifications' Amount"].median()
        small_life_time_mean = dataset_small["Life Time (day)"].mean()
        small_life_time_median = dataset_small["Life Time (day)"].median()
        small_interval_mean = dataset_small["Modifications' Interval (day)"].mean()
        small_interval_median = dataset_small["Modifications' Interval (day)"].median()
        small_deleted_total = len(dataset_small[dataset_small["Deleted?"] == True])
        small_deleted_amount_mean = dataset_small[dataset_small["Deleted?"] == True]["Modifications' Amount"].mean()
        small_deleted_amount_median = dataset_small[dataset_small["Deleted?"] == True]["Modifications' Amount"].median()
        small_deleted_life_time_mean = dataset_small[dataset_small["Deleted?"] == True]["Life Time (day)"].mean()
        small_deleted_life_time_median = dataset_small[dataset_small["Deleted?"] == True]["Life Time (day)"].median()
        small_deleted_interval_mean = dataset_small[dataset_small["Deleted?"] == True]["Modifications' Interval (day)"].mean()
        small_deleted_interval_median = dataset_small[dataset_small["Deleted?"] == True]["Modifications' Interval (day)"].median()

    result = {
        "Large Commits": large_commits,
        "Large Only Added": large_only_added,
        "Large Amount Mean": large_amount_mean,
        "Large Amount Median": large_amount_median,
        "Large Life Time Mean": large_life_time_mean,
        "Large Life Time Median": large_life_time_median,
        "Large Interval Mean": large_interval_mean,
        "Large Interval Median": large_interval_median,
        "Large Deleted Total": large_deleted_total,
        "Large Deleted Amount Mean": large_deleted_amount_mean,
        "Large Deleted Amount Median": large_deleted_amount_median,
        "Large Deleted Life Time Mean": large_deleted_life_time_mean,
        "Large Deleted Life Time Median": large_deleted_life_time_median,
        "Large Deleted Interval Mean": large_deleted_interval_mean,
        "Large Deleted Interval Median": large_deleted_interval_median,

        "Small Commits": small_commits,
        "Small Only Added": small_only_added,
        "Small Amount Mean": small_amount_mean,
        "Small Amount Median": small_amount_median,
        "Small Life Time Mean": small_life_time_mean,
        "Small Life Time Median": small_life_time_median,
        "Small Interval Mean": small_interval_mean,
        "Small Interval Median": small_interval_median,
        "Small Deleted Total": small_deleted_total,
        "Small Deleted Amount Mean": small_deleted_amount_mean,
        "Small Deleted Amount Median": small_deleted_amount_median,
        "Small Deleted Life Time Mean": small_deleted_life_time_mean,
        "Small Deleted Life Time Median": small_deleted_life_time_median,
        "Small Deleted Interval Mean": small_deleted_interval_mean,
        "Small Deleted Interval Median": small_deleted_interval_median,
    }
    return pd.DataFrame(result)

# Processamento principal =====================================================================================

for i, row in repositories.iterrows():
    repo_url: str = row['url']
    language: str = row['main language']
    repo_name: str = repo_url.split('/')[-1]
    repo_owner: str = repo_url.split('/')[-2]
    repo_path: str = f"{language}/{repo_owner}~{repo_name}"

    print(repo_path)

    # Cria diretórios necessários
    makedirs(f"{output_path}/per_project/{language}", exist_ok=True)
    # makedirs(f"{output_path}/per_languages", exist_ok=True)

    # Processa arquivos grandes
    large_df: pd.DataFrame = pd.DataFrame()
    large_path = f"{large_files_commits_path}{repo_path}.csv"
    if path.exists(large_path):
        large_df: pd.DataFrame = pd.read_csv(large_path, sep=SEPARATOR)

    # Processa arquivos pequenos
    small_path = f"{small_files_commits_path}{repo_path}.csv"
    small_df: pd.DataFrame = pd.DataFrame()
    if path.exists(small_path):
        small_df: pd.DataFrame = pd.read_csv(small_path, sep=SEPARATOR)

    project_results: list[pd.DataFrame] = []
    if not large_df.empty:
        project_results.append(frequency_by_lifetime(large_df))
    if not small_df.empty:
        project_results.append(frequency_by_lifetime(small_df, 'small'))

    if project_results:
        pd.concat(project_results).to_csv(f"{output_path}/per_project/{repo_path}.csv", index=False)


# ================================================================================================================
all_df = pd.DataFrame()

current_language: str = None
current_df: pd.DataFrame = pd.DataFrame()

for i, row in repositories.iterrows():
    repo_url: str = row['url']
    language: str = row['main language']
    repo_name: str = repo_url.split('/')[-1]
    repo_owner: str = repo_url.split('/')[-2]
    repo_path: str = f"{language}/{repo_owner}~{repo_name}"

    print(repo_path)

    # Cria diretórios necessários
    makedirs(f"{output_path}general/per_project/{language}", exist_ok=True)
    makedirs(f"{output_path}general/per_language/", exist_ok=True)

    # Atualiza acumuladores de linguagem quando muda
    if current_language and (language != current_language):
        calc_metrics(current_df).to_csv(f"{output_path}general/per_language/{current_language}.csv", index=False)
        all_df = pd.concat([all_df, current_df])
        current_df = pd.DataFrame()

    current_language = language

    # Processa por projeto
    df = pd.DataFrame()
    if path.exists(f"{modify_path}{repo_path}.csv"):
        df = pd.read_csv(f"{modify_path}{repo_path}.csv")
        calc_metrics(df).to_csv(f"{output_path}general/per_project/{repo_path}.csv", index=False)
        current_df = pd.concat([current_df, df])

# Processa última linguagem
if not current_df.empty:
    calc_metrics(current_df).to_csv(f"{output_path}general/per_language/{current_language}.csv", index=False)
    all_df = pd.concat([all_df, current_df])

# Resultado global ============================================================================================
if not all_df.empty:
    calc_metrics(all_df).to_csv(f"{output_path}general/global_results.csv", index=False)
