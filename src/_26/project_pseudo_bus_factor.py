import pandas as pd
from os import makedirs, path
from sys import setrecursionlimit
import datetime
from dateutil import parser  # Add this import for robust datetime parsing

setrecursionlimit(2_000_000)

SEPARATOR = '|'

# Setup =======================================================================================================
input_path: str = "./src/_26/input/"
output_path: str = "./src/_26/output/"

percentil_path: str = "./src/_02/output/percentis_by_language_filtered.csv"
repositories_path: str = "./src/_00/input/450_Starred_Projects.csv"
language_white_list_path: str = "./src/_12/input/white_list.csv"
large_files_commits_path: str = "./src/_10/output/large_files/"
small_files_commits_path: str = "./src/_10/output/small_files/"

# Carrega e ordena repositórios por linguagem
repositories: pd.DataFrame = pd.read_csv(repositories_path).sort_values(by='main language').reset_index(drop=True)

# DataFrames globais
large_files_commits: pd.DataFrame = pd.DataFrame()
small_files_commits: pd.DataFrame = pd.DataFrame()
percentil_df: pd.DataFrame = pd.read_csv(percentil_path)
language_white_list_df: pd.DataFrame = pd.read_csv(language_white_list_path)


# Funções auxiliares =========================================================================================
def pseudo_bus_factor(repository_commits: pd.DataFrame, change_type: str = "large") -> pd.DataFrame:
    """
    Top autores que correspondem a 70% dos commits totais;
    % da intersecção entre o quartil 25% mais novo e mais antigo
    """
    
    # SETUP ===============================================================================================================
    commits_df: pd.DataFrame = repository_commits.copy()
    commits_df_total: int = commits_df['Hash'].nunique()

    if not commits_df.empty:
        commits_df = commits_df[commits_df['File Name'].apply(lambda x: isinstance(x, str) and '.' in x)]
        if not commits_df.empty:
            commits_df['Extension'] = commits_df['File Name'].apply(lambda x: x.split(".")[-1]).copy()
            commits_df = commits_df[commits_df['Extension'].isin(language_white_list_df['Extension'].values)]
            commits_df = commits_df.merge(
                language_white_list_df[['Extension', 'Language']],
                on='Extension',
                how='left'
            ).drop(columns=['Extension'])

    changes = repository_commits[
        repository_commits['Local File PATH New'].isin(commits_df['Local File PATH New'].values) |
        repository_commits['Local File PATH New'].isin(commits_df['Local File PATH Old'].values)
        ].copy()

    # Cria um mapeamento completo de TODOS os caminhos (New e Old) para linguagem
    path_to_language = pd.concat([
        commits_df[['Local File PATH New', 'Language']].rename(columns={'Local File PATH New': 'Path'}),
        commits_df[['Local File PATH Old', 'Language']].rename(columns={'Local File PATH Old': 'Path'})
    ]).dropna(subset=['Path']).set_index('Path')['Language'].to_dict()
    # Atribui a linguagem baseada em ambos os caminhos
    changes['Language'] = changes.apply(
        lambda x: (
            path_to_language.get(x['Local File PATH New']) or 
            path_to_language.get(x['Local File PATH Old'])
        ),
        axis=1
    )
    commits_df_filtered_total:int = commits_df['Hash'].nunique()
    
    # ANAL ================================================================================================================
    # Ordena os commits por data (do mais velho para o mais novo)
    commits_df['Committer Commit Date'] = commits_df.apply(
        lambda x: parser.parse(x['Committer Commit Date']).astimezone(datetime.timezone.utc),
        axis=1
    )
    commits_df = commits_df.sort_values(by='Committer Commit Date')

    # Quantos commits cada autor tem, guarde em um dicionário organizado do autor que mais tem commits para o que menos tem
    author_commit_counts = commits_df['Committer Email'].value_counts()

    # Quantos commits equivalem a 70% de commits
    threshold_70 = 0.7 * commits_df_total

    # Menor número de autores que correspondem a 70% dos commits
    cumulative_commits = author_commit_counts.cumsum()
    top_authors = cumulative_commits[cumulative_commits <= threshold_70].index.tolist()
    num_top_authors = len(top_authors)

    # Autores responsáveis pelos 25% primeiros commits
    first_25_percent = commits_df.iloc[:int(0.25 * len(commits_df))]
    first_25_authors = set(first_25_percent['Committer Email'])

    # Autores responsáveis pelos 25% últimos commits
    last_25_percent = commits_df.iloc[-int(0.25 * len(commits_df)):]
    last_25_authors = set(last_25_percent['Committer Email'])

    # Intersecção os números absolutos e percentuais da intersecção de autores entre os primeiros e os últimos 25%
    intersection_authors = first_25_authors.intersection(last_25_authors)
    intersection_count = len(intersection_authors)
    intersection_percentage = (intersection_count / len(first_25_authors.union(last_25_authors))) * 100 if first_25_authors.union(last_25_authors) else 0

    # Adicione os resultados à variável result
    result: dict = {
        "Type": [change_type],
        "Commits Amount": [commits_df_filtered_total],
        
        "70% Threshould": [num_top_authors],
        "25% Union": [intersection_percentage]
    }

    return pd.DataFrame(result)

def process_language(lang: str, large: pd.DataFrame, small: pd.DataFrame, output_path: str):
    """Processa e salva resultados por linguagem"""
    results:list[pd.DataFrame] = []
    if not large.empty:
        results.append(pseudo_bus_factor(large, 'large'))
    if not small.empty:
        results.append(pseudo_bus_factor(small, 'small'))
    
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
        project_results.append(pseudo_bus_factor(large_df))
    if not small_df.empty:
        project_results.append(pseudo_bus_factor(small_df, 'small'))

    if project_results:
        pd.concat(project_results).to_csv(f"{output_path}/per_project/{repo_path}.csv", index=False)

# Processa última linguagem
if not current_large.empty or not current_small.empty:
    process_language(current_language, current_large, current_small, output_path)

# Resultado global ============================================================================================
final_results: list[pd.DataFrame] = []
if not large_files_commits.empty:
    final_results.append(pseudo_bus_factor(large_files_commits))
if not small_files_commits.empty:
    final_results.append(pseudo_bus_factor(small_files_commits, 'small'))

if final_results:
    pd.concat(final_results).to_csv(f"{output_path}/global_results.csv", index=False)
