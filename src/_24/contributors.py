import pandas as pd
from os import makedirs, path
from sys import setrecursionlimit

setrecursionlimit(2_000_000)

SEPARATOR = '|'

# Setup =======================================================================================================
input_path: str = "./src/_24/input/"
output_path: str = "./src/_24/output/"

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
import numpy as np

def calculate_bus_factor(repository_commits: pd.DataFrame) -> int:
    """
    Calcula o Bus Factor com base nos commits, seguindo o método de Avelino et al.
    
    Args:
        repository_commits: DataFrame com colunas ['Hash', 'Author Email', 'Committer Email', 'Files', 'Date']
                            'Files' deve ser uma lista de arquivos modificados no commit.
                            'Date' deve ser datetime.
    
    Returns:
        Número inteiro representando o Bus Factor.
    """
    # Extrair dados por arquivo e autor
    file_author_data = []
    for _, commit in repository_commits.iterrows():
        author = commit['Author Email']
        files = commit['Files']
        date = commit['Date']
        for file in files:
            file_author_data.append({
                'File': file,
                'Author': author,
                'Date': date
            })
    
    df_files = pd.DataFrame(file_author_data)
    
    # Calcular First Author (FA) para cada arquivo
    first_authors = df_files.groupby('File')['Date'].idxmin()
    df_files['FA'] = 0
    df_files.loc[first_authors, 'FA'] = 1  # Marcar o autor do primeiro commit como FA
    
    # Calcular DL (commits por autor por arquivo)
    dl = df_files.groupby(['File', 'Author']).size().reset_index(name='DL')
    
    # Calcular AC (commits de outros autores por arquivo)
    total_commits_per_file = df_files.groupby('File').size().reset_index(name='TotalCommits')
    ac = pd.merge(dl, total_commits_per_file, on='File')
    ac['AC'] = ac['TotalCommits'] - ac['DL']
    
    # Calcular DOA para cada autor e arquivo (Fórmula simplificada de Avelino et al.)
    ac['DOA'] = 3.293 + 1.098 * ac['FA'] + 0.164 * ac['DL'] - 0.321 * np.log1p(ac['AC'])
    
    # Determinar autores principais (DOA > 3.293 e > 75% do máximo DOA do arquivo)
    max_doa_per_file = ac.groupby('File')['DOA'].transform('max')
    ac['IsMainAuthor'] = (ac['DOA'] > 3.293) & (ac['DOA'] >= 0.75 * max_doa_per_file)
    
    # Lista de arquivos e seus autores principais
    main_authors = ac[ac['IsMainAuthor']][['File', 'Author']]
    
    # Algoritmo iterativo para calcular o Bus Factor
    remaining_files = main_authors['File'].unique()
    bus_factor = 0
    abandoned_files = set()
    authors_removed = set()
    
    while len(abandoned_files) < 0.5 * len(remaining_files):
        # Contar quantos arquivos cada autor é principal
        author_counts = main_authors[~main_authors['Author'].isin(authors_removed)].groupby('Author')['File'].nunique()
        if author_counts.empty:
            break
        top_author = author_counts.idxmax()
        authors_removed.add(top_author)
        bus_factor += 1
        
        # Verificar quais arquivos perderam todos os autores principais
        files_with_top_author = set(main_authors[main_authors['Author'] == top_author]['File'])
        for file in files_with_top_author:
            file_authors = set(main_authors[main_authors['File'] == file]['Author'])
            if file_authors.issubset(authors_removed):
                abandoned_files.add(file)
    
    return bus_factor

def anal_contributors(repository_commits: pd.DataFrame, change_type: str = "large") -> pd.DataFrame:
    """Função Base para o processamento de dados"""
    
    commits = repository_commits.unique('Hash').copy()
    
    authors = commits.groupby('Author Email').copy()
    committer = commits.groupby('Committer Email').copy()
    top_authors = authors.size().nlargest(10).reset_index(name='Contributions')
    top_committers = committer.size().nlargest(10).reset_index(name='Contributions')
    
    author_dicts = []
    for author_email, group in authors:
        author_dicts.append({
            "Email": author_email,
            "Contributions": len(group)
        })
        
    committer_dicts = []
    for committer_email, group in committer:
        committer_dicts.append({
            "Email": committer_email,
            "Contributions": len(group)
        })
    
    result: dict = {
        "Type": [change_type],
        "Result 1": ["Result 1"],
        "Result 2": ["Result 2"]
    }
    return pd.DataFrame(result)

def process_language(lang: str, large: pd.DataFrame, small: pd.DataFrame, output_path: str):
    """Processa e salva resultados por linguagem"""
    results:list[pd.DataFrame] = []
    if not large.empty:
        results.append(anal_contributors(large, 'large'))
    if not small.empty:
        results.append(anal_contributors(small, 'small'))
    
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
        project_results.append(anal_contributors(large_df))
    if not small_df.empty:
        project_results.append(anal_contributors(small_df, 'small'))

    if project_results:
        pd.concat(project_results).to_csv(f"{output_path}/per_project/{repo_path}.csv", index=False)

# Processa última linguagem
if not current_large.empty or not current_small.empty:
    process_language(current_language, current_large, current_small, output_path)

# Resultado global ============================================================================================
final_results: list[pd.DataFrame] = []
if not large_files_commits.empty:
    final_results.append(anal_contributors(large_files_commits))
if not small_files_commits.empty:
    final_results.append(anal_contributors(small_files_commits, 'small'))

if final_results:
    pd.concat(final_results).to_csv(f"{output_path}/global_results.csv", index=False)