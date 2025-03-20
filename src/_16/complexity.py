import pandas as pd
from os import makedirs, path
from sys import setrecursionlimit

setrecursionlimit(2_000_000)

SEPARATOR = '|'

# Setup =======================================================================================================
input_path: str = "./src/_16/input/"
output_path: str = "./src/_16/output/"

repositories_path: str = "./src/_00/input/450_Starred_Projects.csv"
large_files_commits_path: str = "./src/_10/output/large_files/"
small_files_commits_path: str = "./src/_10/output/small_files/"

# Carrega e ordena repositórios por linguagem
repositories: pd.DataFrame = pd.read_csv(repositories_path).sort_values(by='main language').reset_index(drop=True)

# DataFrames globais
large_files_commits: pd.DataFrame = pd.DataFrame()
small_files_commits: pd.DataFrame = pd.DataFrame()

# Funções auxiliares =========================================================================================
def major_complexities(repository_commits: pd.DataFrame, change_type: str = "large") -> pd.DataFrame:
    """Commits com maiores adições/remoções por foram identificados e sua complexidade"""
    df = repository_commits[repository_commits['Complexity'] != 'not calculated'].copy()
    not_calculated = repository_commits[repository_commits['Complexity'] == 'not calculated']
    if df.empty:
        return None
    df['Complexity'] = df['Complexity'].apply(lambda x: int(x))
    df['Lines Balance'] = df['Lines Added'] - df['Lines Deleted']
    
    added_max_row = df.loc[df['Lines Balance'].idxmax()]
    deleted_max_row = df.loc[df['Lines Balance'].idxmin()]
    
    value_major_complexity = df['Complexity'].max()
    major_complexity_added = df[(df['Complexity'] == value_major_complexity) & (df['Lines Balance'] == df['Lines Balance'].max())].iloc[0]
    major_complexity_deleted = df[(df['Complexity'] == value_major_complexity) & (df['Lines Balance'] == df['Lines Balance'].min())].iloc[0]

    result = {
        "Change Type": [change_type],
        "Average Complexity": [df['Complexity'].mean()],
        "Median Complexity": [df['Complexity'].median()],
        
        "Highest Complexity Added": [major_complexity_added['Complexity']],
        "Largest Modification Balance Added": [major_complexity_added['Lines Balance']],
        "Project with Highest Complexity Added": [major_complexity_added['Project Name']],
        "File with Highest Complexity Added": [major_complexity_added['Local File PATH New']],
        "Largest Modification Hash Added": [major_complexity_added['Hash']],
        
        "Highest Complexity Deleted": [major_complexity_deleted['Complexity']],
        "Largest Modification Balance Deleted": [major_complexity_deleted['Lines Balance']],
        "Project with Highest Complexity Deleted": [major_complexity_deleted['Project Name']],
        "File with Highest Complexity Deleted": [major_complexity_deleted['Local File PATH New']],
        "Largest Modification Hash Deleted": [major_complexity_deleted['Hash']],
        
        "Max Lines Added": [added_max_row['Lines Balance']],
        "Max Lines Added Hash": [added_max_row['Hash']],
        "Complexity of Max Lines Added": [added_max_row['Complexity']],
        "Project with Max Lines Added": [added_max_row['Project Name']],
        "File with Max Lines Added": [added_max_row['Local File PATH New']],
        
        "Max Lines Deleted": [deleted_max_row['Lines Balance']],
        "Max Lines Deleted Hash": [deleted_max_row['Hash']],
        "Complexity of Max Lines Deleted": [deleted_max_row['Complexity']],
        "Project with Max Lines Deleted": [deleted_max_row['Project Name']],
        "File with Max Lines Deleted": [deleted_max_row['Local File PATH New']],
        
        "Not Calculated Count": [len(not_calculated)]
    }
    return pd.DataFrame(result)

def process_language(lang: str, large: pd.DataFrame, small: pd.DataFrame, output_path: str):
    """Processa e salva resultados por linguagem"""
    results:list[pd.DataFrame] = []
    if not large.empty:
        results.append(major_complexities(large, 'large'))
    if not small.empty:
        results.append(major_complexities(small, 'small'))
    
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
        project_results.append(major_complexities(large_df))
    if not small_df.empty:
        project_results.append(major_complexities(small_df, 'small'))

    if project_results:
        pd.concat(project_results).to_csv(f"{output_path}/per_project/{repo_path}.csv", index=False)

# Processa última linguagem
if not current_large.empty or not current_small.empty:
    process_language(current_language, current_large, current_small, output_path)

# Resultado global ============================================================================================
final_results: list[pd.DataFrame] = []
if not large_files_commits.empty:
    final_results.append(major_complexities(large_files_commits))
if not small_files_commits.empty:
    final_results.append(major_complexities(small_files_commits, 'small'))

if final_results:
    pd.concat(final_results).to_csv(f"{output_path}/global_results.csv", index=False)