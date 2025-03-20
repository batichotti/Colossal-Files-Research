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
    """Identifica commits com maiores adições/remoções e sua complexidade."""
    df = repository_commits[repository_commits['Change Type'] == 'MODIFY'].copy()
    df = df[df['Complexity'] != 'not calculated']
    total_calculated = len(df)
    not_calculated = repository_commits[repository_commits['Complexity'] == 'not calculated']
    
    if df.empty:
        return pd.DataFrame({"Change Type": [change_type], "Not Calculated Count": [len(not_calculated)]}) if not_calculated.any().any() else None
    
    df['Complexity'] = pd.to_numeric(df['Complexity'], errors='coerce').astype('Int64')
    df['Lines Balance'] = df['Lines Added'] - df['Lines Deleted']
    df = df.dropna(subset=['Complexity', 'Lines Balance'])
    
    if df.empty:
        return pd.DataFrame({"Change Type": [change_type], "Not Calculated Count": [len(not_calculated)]}) if not_calculated.any().any() else None
    
    # Maior complexidade com maior Lines Balance
    max_complexity = df['Complexity'].max()
    major_add = df[df['Complexity'] == max_complexity].nlargest(1, 'Lines Balance')
    major_del = df[df['Complexity'] == max_complexity].nsmallest(1, 'Lines Balance')
    
    # Maiores modificações globais
    max_add_row = df.nlargest(1, 'Lines Balance')
    max_del_row = df.nsmallest(1, 'Lines Balance')
    
    # Construir resultado
    result = {
        "Change Type": [change_type],

        "Calculated Count": [total_calculated],
        "Not Calculated Count": [len(not_calculated)],
        "Average Complexity": [df['Complexity'].mean()],
        "Median Complexity": [df['Complexity'].median()],
        "Highest Complexity": [df['Complexity'].max()],
        
        # Dados da maior complexidade com adição
        "Highest Complexity for Added Lines": [major_add['Complexity'].iloc[0] if not major_add.empty else None],
        "Lines Added": [major_add['Lines Balance'].iloc[0] if not major_add.empty else None],
        "Project Added": [major_add['Project Name'].iloc[0] if not major_add.empty else None],
        "File Added": [major_add['Local File PATH New'].iloc[0] if not major_add.empty else None],
        "Hash Added": [major_add['Hash'].iloc[0] if not major_add.empty else None],
        
        # Dados da maior complexidade com remoção
        "Highest Complexity for Deleted Lines": [major_del['Complexity'].iloc[0] if not major_del.empty else None],
        "Lines Deleted": [major_del['Lines Balance'].iloc[0] if not major_del.empty else None],
        "Project Deleted": [major_del['Project Name'].iloc[0] if not major_del.empty else None],
        "File Deleted": [major_del['Local File PATH New'].iloc[0] if not major_del.empty else None],
        "Hash Deleted": [major_del['Hash'].iloc[0] if not major_del.empty else None],
        
        # Maiores modificações absolutas
        "Max Lines Added": [max_add_row['Complexity'].iloc[0] if not max_add_row.empty else None],
        "Max Lines Added Complexity": [max_add_row['Lines Balance'].iloc[0] if not max_add_row.empty else None],
        "Max Lines Added Project": [max_add_row['Project Name'].iloc[0] if not max_add_row.empty else None],
        "Max Lines Added File": [max_add_row['Local File PATH New'].iloc[0] if not max_add_row.empty else None],
        "Max Lines Added Hash": [max_add_row['Hash'].iloc[0] if not max_add_row.empty else None],
        
        "Max Lines Deleted": [max_del_row['Complexity'].iloc[0] if not max_del_row.empty else None],
        "Max Lines Deleted Complexity": [max_del_row['Lines Balance'].iloc[0] if not max_del_row.empty else None],
        "Max Lines Deleted Project": [max_del_row['Project Name'].iloc[0] if not max_del_row.empty else None],
        "Max Lines Deleted File": [max_del_row['Local File PATH New'].iloc[0] if not max_del_row.empty else None],
        "Max Lines Deleted Hash": [max_del_row['Hash'].iloc[0] if not max_del_row.empty else None]
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