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

    born_large = repository_commits[repository_commits['Change Type'] == 'ADD']
    babies_total: int = len(born_large)

    born_large['Extension'] = born_large['Local File PATH New'].apply(lambda x: x.split("/")[-1].split(".")[-1])
    born_large = born_large[born_large['Extension'].isin(language_white_list_df['Extension'].values)]

    born_large = born_large.merge(
        language_white_list_df[['Extension', 'Language']],
        on='Extension',
        how='left'
    ).drop(columns=['Extension'])

    born_large = born_large.dropna(subset=['Language'])
    born_large = born_large[born_large['Lines Of Code (nloc)'] != "not calculated"]

    # Filtra as linhas onde a linguagem é igual e o número de linhas de código é menor que o percentil 99
    percentil_99 = percentil_df.set_index('language')['percentil 99']
    born_large = born_large[born_large['Lines Of Code (nloc)'] >= born_large['Language'].map(percentil_99)]
    
    result: dict = {
        "Type": [change_type],
        "Files Added TOTAL": [babies_total],
        "Large Files Added TOTAL": [len(born_large)],
        "Large Files Added Percentage": [(len(born_large)/babies_total)*100]
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