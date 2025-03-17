import pandas as pd
from os import makedirs, path
from sys import setrecursionlimit

setrecursionlimit(2_000_000)

SEPARATOR = '|'

# Setup =======================================================================================================
input_path: str = "./src/_15/input/"
output_path: str = "./src/_15/output/"

repositories_path: str = "./src/_00/input/450_Starred_Projects.csv"
percentil_path: str = "./src/_02/output/percentis_by_language_filtered.csv"
language_white_list_path: str = f"{input_path}/white_list.csv"
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
def born_or_become(repository_commits: pd.DataFrame, change_type: str = "large") -> pd.DataFrame:
    """Detecta se um arquivo nasceu grande ou se ele se tornou"""

    files_total = len(repository_commits.groupby('Local File PATH New'))

    born_large = repository_commits[repository_commits['Change Type'] == 'ADD'].copy()
    babies_total: int = len(born_large)

    born_large['Extension'] = born_large['File Name'].apply(lambda x: x.split(".")[-1])
    born_large = born_large[born_large['Extension'].isin(language_white_list_df['Extension'].values)]

    born_large = born_large.merge(
        language_white_list_df[['Extension', 'Language']],
        on='Extension',
        how='left'
    ).drop(columns=['Extension'])

    # Converte NLOC para numérico e remove inválidos
    born_large['Lines Of Code (nloc)'] = pd.to_numeric(born_large['Lines Of Code (nloc)'], errors='coerce')
    born_large = born_large.dropna(subset=['Language', 'Lines Of Code (nloc)'])

    # Filtra as linhas onde a linguagem é igual e o número de linhas de código é menor que o percentil 99
    percentil_99 = percentil_df.set_index('language')['percentil 99']
    born_large = born_large[born_large.apply(
        lambda x: x['Lines Of Code (nloc)'] >= percentil_99.get(x['Language'], 0), 
        axis=1
    )]

    born_large = born_large.sort_values(by='Committer Commit Date')

    born_large.to_csv(f"{output_path}/per_project/{repo_path}_{type}s_commits_born.csv", index=False)

    # BECOME
    become_large = repository_commits[repository_commits['Change Type'] == 'MODIFY'].copy()
    modifieds_total = len(become_large.groupby('Local File PATH New'))

    become_large['Extension'] = become_large['File Name'].apply(lambda x: x.split(".")[-1])
    become_large = become_large[become_large['Extension'].isin(language_white_list_df['Extension'].values)]

    become_large = become_large.merge(
        language_white_list_df[['Extension', 'Language']],
        on='Extension',
        how='left'
    ).drop(columns=['Extension'])

    # Converte NLOC e remove inválidos
    become_large['Lines Of Code (nloc)'] = pd.to_numeric(become_large['Lines Of Code (nloc)'], errors='coerce')
    become_large = become_large.dropna(subset=['Language', 'Lines Of Code (nloc)'])

    # Filtra pelo percentil
    become_large = become_large[become_large.apply(
        lambda x: x['Lines Of Code (nloc)'] >= percentil_99.get(x['Language'], 0), 
        axis=1
    )]

    become_large = become_large.sort_values(by='Committer Commit Date')

    become_large.to_csv(f"{output_path}/per_project/{repo_path}_{type}s_commits_become.csv", index=False)

    become_large_per_file = become_large.groupby('Local File PATH New')

    # FLEX LARGE
    flex_large = repository_commits[repository_commits['Local File PATH New'].isin(
        pd.concat([born_large['Local File PATH New'], become_large['Local File PATH New']])
    )].copy()

    flex_large = flex_large[~flex_large['Local File PATH New'].isin(
        pd.concat([born_large['Local File PATH New'], become_large['Local File PATH New']])
    )]

    flex_large = flex_large.sort_values(by='Committer Commit Date')

    flex_large_grouped = flex_large.groupby('Local File PATH New')

    flex_large.to_csv(f"{output_path}/per_project/{repo_path}_{change_type}s_flex_large.csv", index=False)

    # NO LONGER LARGE
    no_longer_large = repository_commits[repository_commits['Local File PATH New'].isin(
        pd.concat([born_large['Local File PATH New'], become_large['Local File PATH New']])
    )].copy()

    no_longer_large = no_longer_large.sort_values(by='Committer Commit Date')

    no_longer_large = no_longer_large[~no_longer_large['Local File PATH New'].isin(
        pd.concat([born_large['Local File PATH New'], become_large['Local File PATH New']])
    )]

    no_longer_large_grouped = no_longer_large.groupby('Local File PATH New')

    remaining_no_longer = []
    for file_path, group in no_longer_large_grouped:
        last_commit_date = group['Committer Commit Date'].max()
        # born
        if file_path in born_large['Local File PATH New'].values:
            born_last_commit_date = born_large[born_large['Local File PATH New'] == file_path]['Committer Commit Date'].max()
        else:
            born_last_commit_date = pd.Timestamp.min
        # become
        if file_path in become_large_per_file.groups:
            become_last_commit_date = become_large_per_file.get_group(file_path)['Committer Commit Date'].max()
        else:
            become_last_commit_date = pd.Timestamp.min
        #comparação de data
        if last_commit_date > born_last_commit_date and last_commit_date > become_last_commit_date:
            remaining_no_longer.append(group)

    no_longer:pd.DataFrame
    if remaining_no_longer:
        no_longer = pd.concat(remaining_no_longer)
        no_longer.to_csv(f"{output_path}/per_project/{repo_path}_{change_type}s_no_longer.csv", index=False)

    result: dict = {
        "Type": [change_type],
        "Added Files TOTAL": [babies_total],
        "Added Large Files TOTAL": [len(born_large)],
        "Added Large Files Percentage": [(len(born_large)/babies_total)*100],
        "Modified Files TOTAL": [modifieds_total],
        "Modified Large Files TOTAL": [len(become_large_per_file)],
        "Modified Large Files Percentage": [(len(become_large_per_file)/modifieds_total)*100],
        "Flex Large Files TOTAL": [len(flex_large_grouped)],
        "Flex Large Files Percentage": [(len(flex_large_grouped)/files_total)*100],
        "No Longer Large Files TOTAL": [len(remaining_no_longer)],
        "No Longer Large Files Percentage": [(len(remaining_no_longer)/files_total)*100]
    }
    return pd.DataFrame(result)

def process_language(lang: str, large: pd.DataFrame, small: pd.DataFrame, output_path: str):
    """Processa e salva resultados por linguagem"""
    results:list[pd.DataFrame] = []
    if not large.empty:
        results.append(born_or_become(large, 'large'))
    if not small.empty:
        results.append(born_or_become(small, 'small'))
    
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
        project_results.append(born_or_become(large_df))
    if not small_df.empty:
        project_results.append(born_or_become(small_df, 'small'))

    if project_results:
        pd.concat(project_results).to_csv(f"{output_path}/per_project/{repo_path}.csv", index=False)

# Processa última linguagem
if not current_large.empty or not current_small.empty:
    process_language(current_language, current_large, current_small, output_path)

# Resultado global ============================================================================================
final_results: list[pd.DataFrame] = []
if not large_files_commits.empty:
    final_results.append(born_or_become(large_files_commits))
if not small_files_commits.empty:
    final_results.append(born_or_become(small_files_commits, 'small'))

if final_results:
    pd.concat(final_results).to_csv(f"{output_path}/global_results.csv", index=False)