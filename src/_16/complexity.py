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
import pandas as pd

def major_complexities(repository_commits: pd.DataFrame, change_type: str = "large") -> pd.DataFrame:
    """Identifica commits com maiores adições/remoções e sua complexidade."""
    # Filtrar entradas onde a complexidade não é 'not calculated'
    df_filtered = repository_commits[repository_commits['Complexity'] != 'not calculated'].copy()
    not_calculated = repository_commits[repository_commits['Complexity'] == 'not calculated']
    
    if df_filtered.empty:
        return pd.DataFrame({
            "Change Type": [change_type],
            "Not Calculated Count": [len(not_calculated)]
        }) if len(not_calculated) > 0 else None
    
    # Converter Complexidade para numérico e remover valores inválidos
    df_filtered['Complexity'] = pd.to_numeric(df_filtered['Complexity'], errors='coerce')
    df_filtered = df_filtered.dropna(subset=['Complexity'])
    df_filtered['Complexity'] = df_filtered['Complexity'].astype(int)
    
    # Calcular Lines Balance e remover NaNs
    df_filtered['Lines Balance'] = df_filtered['Lines Added'] - df_filtered['Lines Deleted']
    df_filtered = df_filtered.dropna(subset=['Lines Balance'])
    
    if df_filtered.empty:
        return pd.DataFrame({
            "Change Type": [change_type],
            "Not Calculated Count": [len(not_calculated)]
        }) if len(not_calculated) > 0 else None
    
    # Encontrar as linhas com maior e menor Lines Balance
    try:
        added_max_row = df_filtered.loc[df_filtered['Lines Balance'].idxmax()]
    except ValueError:
        added_max_row = None
    try:
        deleted_max_row = df_filtered.loc[df_filtered['Lines Balance'].idxmin()]
    except ValueError:
        deleted_max_row = None
    
    # Encontrar a maior complexidade e suas ocorrências
    max_complexity = df_filtered['Complexity'].max()
    max_complex_group = df_filtered[df_filtered['Complexity'] == max_complexity]
    
    if max_complex_group.empty:
        return pd.DataFrame({
            "Change Type": [change_type],
            "Not Calculated Count": [len(not_calculated)]
        }) if len(not_calculated) > 0 else None
    
    max_add = max_complex_group['Lines Balance'].max()
    min_delete = max_complex_group['Lines Balance'].min()
    
    major_add = max_complex_group[max_complex_group['Lines Balance'] == max_add]
    major_complexity_added = major_add.iloc[0] if not major_add.empty else None
    
    major_delete = max_complex_group[max_complex_group['Lines Balance'] == min_delete]
    major_complexity_deleted = major_delete.iloc[0] if not major_delete.empty else None
    
    # Construir o resultado com verificações de segurança
    result = {
        "Change Type": [change_type],
        "Average Complexity": [df_filtered['Complexity'].mean()],
        "Median Complexity": [df_filtered['Complexity'].median()],
        "Not Calculated Count": [len(not_calculated)]
    }
    
    # Função auxiliar para preencher dados ou None
    def get_value(row, column, default=None):
        return row[column] if row is not None and column in row else default
    
    # Preencher dados para major_complexity_added
    result.update({
        "Highest Complexity Added": [get_value(major_complexity_added, 'Complexity')],
        "Largest Modification Balance Added": [get_value(major_complexity_added, 'Lines Balance')],
        "Project with Highest Complexity Added": [get_value(major_complexity_added, 'Project Name')],
        "File with Highest Complexity Added": [get_value(major_complexity_added, 'Local File PATH New')],
        "Largest Modification Hash Added": [get_value(major_complexity_added, 'Hash')],
    })
    
    # Preencher dados para major_complexity_deleted
    result.update({
        "Highest Complexity Deleted": [get_value(major_complexity_deleted, 'Complexity')],
        "Largest Modification Balance Deleted": [get_value(major_complexity_deleted, 'Lines Balance')],
        "Project with Highest Complexity Deleted": [get_value(major_complexity_deleted, 'Project Name')],
        "File with Highest Complexity Deleted": [get_value(major_complexity_deleted, 'Local File PATH New')],
        "Largest Modification Hash Deleted": [get_value(major_complexity_deleted, 'Hash')],
    })
    
    # Preencher dados para added_max_row
    result.update({
        "Max Lines Added": [get_value(added_max_row, 'Lines Balance')],
        "Max Lines Added Hash": [get_value(added_max_row, 'Hash')],
        "Complexity of Max Lines Added": [get_value(added_max_row, 'Complexity')],
        "Project with Max Lines Added": [get_value(added_max_row, 'Project Name')],
        "File with Max Lines Added": [get_value(added_max_row, 'Local File PATH New')],
    })
    
    # Preencher dados para deleted_max_row
    result.update({
        "Max Lines Deleted": [get_value(deleted_max_row, 'Lines Balance')],
        "Max Lines Deleted Hash": [get_value(deleted_max_row, 'Hash')],
        "Complexity of Max Lines Deleted": [get_value(deleted_max_row, 'Complexity')],
        "Project with Max Lines Deleted": [get_value(deleted_max_row, 'Project Name')],
        "File with Max Lines Deleted": [get_value(deleted_max_row, 'Local File PATH New')],
    })
    
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