import pandas as pd
from os import makedirs, path
from sys import setrecursionlimit

setrecursionlimit(2_000_000)

SEPARATOR = '|'

# Setup =======================================================================================================
input_path: str = "./src/_12/input/"
output_path: str = "./src/_12/output/"

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
def calc_lines_changes(repository_commits: pd.DataFrame, change_type: str = "large") -> pd.DataFrame:
    """Calcula métricas de alterações de linhas para commits do tipo MODIFY"""

    changes = repository_commits[repository_commits['Change Type'] == 'MODIFY'].copy()

    if not changes.empty:
        changes = changes[changes['File Name'].apply(lambda x: isinstance(x, str) and '.' in x)]
        if not changes.empty:
            changes['Extension'] = changes['File Name'].apply(lambda x: x.split(".")[-1]).copy()
            changes = changes[changes['Extension'].isin(language_white_list_df['Extension'].values)]
            added_files = added_files.merge(
                language_white_list_df[['Extension', 'Language']],
                on='Extension',
                how='left'
            ).drop(columns=['Extension'])

    changes_large: pd.DataFrame = pd.DataFrame()
    changes_small: pd.DataFrame = pd.DataFrame()

    if not changes.empty:
        changes['Lines Balance'] = changes['Lines Added'] - changes['Lines Deleted']

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

    # Calcs ================================================================================================================
    added_avg = changes[changes['Lines Balance'] > 0]['Lines Balance'].mean() if not changes.empty else 0
    added_median = changes[changes['Lines Balance'] > 0]['Lines Balance'].median() if not changes.empty else 0
    added_max = changes[changes['Lines Balance'] > 0]['Lines Balance'].max() if not changes.empty else 0
    deleted_avg = changes[changes['Lines Balance'] < 0]['Lines Balance'].mean() if not changes.empty else 0
    deleted_median = changes[changes['Lines Balance'] < 0]['Lines Balance'].median() if not changes.empty else 0
    deleted_min = changes[changes['Lines Balance'] < 0]['Lines Balance'].min() if not changes.empty else 0
    zero_count = len(changes[changes['Lines Balance'] == 0]) if not changes.empty else 0

    added_large_avg = changes_large[changes_large['Lines Balance'] > 0]['Lines Balance'].mean() if not changes_large.empty else 0
    added_large_median = changes_large[changes_large['Lines Balance'] > 0]['Lines Balance'].median() if not changes_large.empty else 0
    added_large_max = changes_large[changes_large['Lines Balance'] > 0]['Lines Balance'].max() if not changes_large.empty else 0
    deleted_large_avg = changes_large[changes_large['Lines Balance'] < 0]['Lines Balance'].mean() if not changes_large.empty else 0
    deleted_large_median = changes_large[changes_large['Lines Balance'] < 0]['Lines Balance'].median() if not changes_large.empty else 0
    deleted_large_min = changes_large[changes_large['Lines Balance'] < 0]['Lines Balance'].min() if not changes_large.empty else 0
    zero_large_count = len(changes_large[changes_large['Lines Balance'] == 0]) if not changes_large.empty else 0

    added_small_avg = changes_small[changes_small['Lines Balance'] > 0]['Lines Balance'].mean() if not changes_small.empty else 0
    added_small_median = changes_small[changes_small['Lines Balance'] > 0]['Lines Balance'].median() if not changes_small.empty else 0
    added_small_max = changes_small[changes_small['Lines Balance'] > 0]['Lines Balance'].max() if not changes_small.empty else 0
    deleted_small_avg = changes_small[changes_small['Lines Balance'] < 0]['Lines Balance'].mean() if not changes_small.empty else 0
    deleted_small_median = changes_small[changes_small['Lines Balance'] < 0]['Lines Balance'].median() if not changes_small.empty else 0
    deleted_small_min = changes_small[changes_small['Lines Balance'] < 0]['Lines Balance'].min() if not changes_small.empty else 0
    zero_small_count = len(changes_small[changes_small['Lines Balance'] == 0]) if not changes_small.empty else 0

    result = [{
        "Type": [change_type],
        "Added Average": [added_avg],
        "Added Median": [added_median],
        "Added Max": [added_max],
        "Deleted Average": [deleted_avg],
        "Deleted Median": [deleted_median],
        "Deleted Min": [deleted_min],
        "Zero Count": [zero_count],
        # large
        "Large Added Average": [added_large_avg],
        "Large Added Median": [added_large_median],
        "Large Added Max": [added_large_max],
        "Large Deleted Average": [deleted_large_avg],
        "Large Deleted Median": [deleted_large_median],
        "Large Deleted Min": [deleted_large_min],
        "Large Zero Count": [zero_large_count],
        # small
        "Small Added Average": [added_small_avg],
        "Small Added Median": [added_small_median],
        "Small Added Max": [added_small_max],
        "Small Deleted Average": [deleted_small_avg],
        "Small Deleted Median": [deleted_small_median],
        "Small Deleted Min": [deleted_small_min],
        "Small Zero Count": [zero_small_count]
    }]
    return pd.DataFrame(result)

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
        project_results.append(calc_lines_changes(large_df))
    if not small_df.empty:
        project_results.append(calc_lines_changes(small_df, 'small'))

    if project_results:
        pd.concat(project_results).to_csv(f"{output_path}/per_project/{repo_path}.csv", index=False)

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