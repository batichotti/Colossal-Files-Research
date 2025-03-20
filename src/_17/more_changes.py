import pandas as pd
from os import makedirs, path
from sys import setrecursionlimit

setrecursionlimit(2_000_000)

SEPARATOR = '|'

# Setup =======================================================================================================
input_path: str = "./src/_17/input/"
output_path: str = "./src/_17/output/"

repositories_path: str = "./src/_00/input/450_Starred_Projects.csv"
large_files_commits_path: str = "./src/_10/output/large_files/"
small_files_commits_path: str = "./src/_10/output/small_files/"
language_white_list_path: str = "./src/_15/input/white_list.csv"
percentil_path: str = "./src/_02/output/percentis_by_language_filtered.csv"

# Carrega e ordena repositórios por linguagem
repositories: pd.DataFrame = pd.read_csv(repositories_path).sort_values(by='main language').reset_index(drop=True)
repositories: pd.DataFrame = pd.read_csv(repositories_path).sort_values(by='main language').reset_index(drop=True)
percentil_df: pd.DataFrame = pd.read_csv(percentil_path)
language_white_list_df: pd.DataFrame = pd.read_csv(language_white_list_path)

# DataFrames globais
large_files_commits: pd.DataFrame = pd.DataFrame()
small_files_commits: pd.DataFrame = pd.DataFrame()

# Funções auxiliares =========================================================================================
def changes_counter(repository_commits: pd.DataFrame, change_type: str = "large") -> pd.DataFrame:
    """Função Base para o processamento de dados"""
    changes = repository_commits[repository_commits['Change Type'] == 'MODIFY'].copy()
    max_changes = changes.groupby('Local File PATH New').size()
    max_changes_idx = max_changes.idxmax()
    
    changes_large = changes.copy()
    changes_large = changes_large[changes_large['File Name'].apply(lambda x: isinstance(x, str) and '.' in x)]
    if not changes_large.empty:
        changes_large['Extension'] = changes_large['File Name'].apply(lambda x: x.split(".")[-1]).copy()
        changes_large = changes_large[changes_large['Extension'].isin(language_white_list_df['Extension'].values)]

        changes_large = changes_large.merge(
            language_white_list_df[['Extension', 'Language']],
            on='Extension',
            how='left'
        ).drop(columns=['Extension'])

        # Converte NLOC para numérico e remove inválidos
        changes_large['Lines Of Code (nloc)'] = pd.to_numeric(changes_large['Lines Of Code (nloc)'], errors='coerce')
        changes_large = changes_large.dropna(subset=['Language', 'Lines Of Code (nloc)'])

        # Filtra as linhas onde a linguagem é igual e o número de linhas de código é menor que o percentil 99
        percentil_99 = percentil_df.set_index('language')['percentil 99']
        changes_large = changes_large[changes_large.apply(
            lambda x: x['Lines Of Code (nloc)'] >= percentil_99.get(x['Language'], 0), 
            axis=1
        )]

    max_changes_large = None
    max_changes_small = None
    max_changes_large_idx = None
    max_changes_small_idx = None
    changes_small = pd.DataFrame()
    if not changes_large.empty:
        changes_small = changes[~changes['Local File PATH New'].isin(changes_large['Local File PATH New'].values)].copy()
        max_changes_large = changes_large.groupby('Local File PATH New').size()
        max_changes_large_idx = max_changes_large.idxmax()
        
    if not changes_small.empty:
        changes_large = changes_large[changes_large['File Name'].apply(lambda x: isinstance(x, str) and '.' in x)]
        if not changes_small.empty:
            changes_small['Extension'] = changes_small['File Name'].apply(lambda x: x.split(".")[-1]).copy()
            changes_small = changes_small[changes_small['Extension'].isin(language_white_list_df['Extension'].values)]
            changes_small = changes_small.merge(
                language_white_list_df[['Extension', 'Language']],
                on='Extension',
                how='left'
            ).drop(columns=['Extension'])
        
            max_changes_small = changes_large.groupby('Local File PATH New').size()
            max_changes_small_idx = max_changes_small.idxmax()

    result: dict = {
        "Type": [change_type],
        "#Changes": [max_changes.max()],
        "Project Name": [changes.loc[changes['Local File PATH New'] == max_changes_idx, 'Project Name'].values[0]],
        "File Path": [max_changes_idx],
        
        "#Changes Large": [max_changes_large.max() if max_changes_large is not None else 'Nothing'],
        "Project Name Large": [changes_large.loc[changes_large['Local File PATH New'] == max_changes_large_idx, 'Project Name'].values[0]] if max_changes_large_idx is not None else 'Nothing',
        "File Path Large": [max_changes_large_idx if max_changes_large_idx is not None else 'Nothing'],
        
        "#Changes Small": [max_changes_small.max() if max_changes_small is not None else 'Nothing'],
        "Project Name Small": [changes.loc[changes['Local File PATH New'] == max_changes_small_idx, 'Project Name'].values[0]] if max_changes_small_idx is not None else 'Nothing',
        "File Path Small": [max_changes_small_idx if max_changes_small_idx is not None else 'Nothing']
    }
    
    return pd.DataFrame(result)

def process_language(lang: str, large: pd.DataFrame, small: pd.DataFrame, output_path: str):
    """Processa e salva resultados por linguagem"""
    results:list[pd.DataFrame] = []
    if not large.empty:
        results.append(changes_counter(large, 'large'))
    if not small.empty:
        results.append(changes_counter(small, 'small'))
    
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
        project_results.append(changes_counter(large_df))
    if not small_df.empty:
        project_results.append(changes_counter(small_df, 'small'))

    if project_results:
        pd.concat(project_results).to_csv(f"{output_path}/per_project/{repo_path}.csv", index=False)

# Processa última linguagem
if not current_large.empty or not current_small.empty:
    process_language(current_language, current_large, current_small, output_path)

# Resultado global ============================================================================================
final_results: list[pd.DataFrame] = []
if not large_files_commits.empty:
    final_results.append(changes_counter(large_files_commits))
if not small_files_commits.empty:
    final_results.append(changes_counter(small_files_commits, 'small'))

if final_results:
    pd.concat(final_results).to_csv(f"{output_path}/global_results.csv", index=False)