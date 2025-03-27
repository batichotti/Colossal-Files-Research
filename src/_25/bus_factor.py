import pandas as pd
from os import makedirs, path
from sys import setrecursionlimit
import numpy as np
import datetime
from math import log

setrecursionlimit(2_000_000)

SEPARATOR = '|'

# Setup =======================================================================================================
input_path: str = "./src/_25/input/"
output_path: str = "./src/_25/output/"

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

### Função para o Método de Avelino et al. ###
def calculate_bus_factor_avelino(changes: pd.DataFrame) -> int:
    if changes.empty:
        return 0
    
    # Preprocessamento dos arquivos (como no seu código original)
    changes['File Path'] = changes.apply(
        lambda x: x['Local File PATH New'] if pd.notna(x['Local File PATH New']) 
                else x['Local File PATH Old'], 
        axis=1
    )
    changes_files_total = changes['File Path'].nunique()
    
    # Encontrar criadores de arquivos (FA = 1)
    first_commit_per_file = changes.groupby('File Path')['Committer Commit Date'].idxmin()
    file_creators = changes.loc[first_commit_per_file, ['File Path', 'Committer Email']]
    file_creators_dict = file_creators.set_index('File Path')['Committer Email'].to_dict()
    
    # Calcular DOA para cada desenvolvedor e arquivo
    doa_data = []
    for file_path, file_group in changes.groupby('File Path'):
        creator = file_creators_dict.get(file_path, None)
        total_commits = len(file_group)
        
        for dev, dev_group in file_group.groupby('Committer Email'):
            dl = len(dev_group)  # Commits do desenvolvedor
            ac = total_commits - dl  # Commits de outros
            fa = 1 if dev == creator else 0
            
            doa = 3.293 + 1.098 * fa + 0.164 * dl - 0.321 * log(1 + ac)
            doa_data.append({
                'File Path': file_path,
                'Committer Email': dev,
                'DOA': doa
            })
    
    doa_df = pd.DataFrame(doa_data)
    
    # Identificar autores por arquivo (critério de Avelino)
    authors = []
    for file_path, file_group in doa_df.groupby('File Path'):
        max_doa = file_group['DOA'].max()
        threshold = max(3.293, 0.75 * max_doa)
        file_authors = file_group[file_group['DOA'] > threshold]['Committer Email'].tolist()
        authors.extend([(file_path, author) for author in file_authors])
    
    authors_df = pd.DataFrame(authors, columns=['File Path', 'Committer Email'])
    
    # Simular remoção iterativa de autores
    developer_files = authors_df.groupby('Committer Email')['File Path'].nunique()
    sorted_devs = developer_files.sort_values(ascending=False).index.tolist()
    
    abandoned_files = set()
    bus_factor = 0
    
    for dev in sorted_devs:
        bus_factor += 1
        dev_files = authors_df[authors_df['Committer Email'] == dev]['File Path'].tolist()
        abandoned_files.update(dev_files)
        
        if len(abandoned_files) / changes_files_total > 0.5:
            return bus_factor
    
    return bus_factor

### Função para o Método de Cosentino et al. (M2) ###
def calculate_bus_factor_cosentino(changes: pd.DataFrame) -> int:
    if changes.empty:
        return 0
    
    # Preprocessamento (igual ao método Avelino)
    changes['File Path'] = changes.apply(
        lambda x: x['Local File PATH New'] if pd.notna(x['Local File PATH New']) 
                else x['Local File PATH Old'], 
        axis=1
    )
    changes_files_total = changes['File Path'].nunique()
    
    # Calcular contribuição proporcional (M2)
    contribution_data = []
    for file_path, file_group in changes.groupby('File Path'):
        total_commits = len(file_group)
        if total_commits == 0:
            continue
        
        for dev, dev_group in file_group.groupby('Committer Email'):
            share = (len(dev_group) / total_commits) * 100
            contribution_data.append({
                'File Path': file_path,
                'Committer Email': dev,
                'Share': share
            })
    
    contribution_df = pd.DataFrame(contribution_data)
    
    # Identificar primários e secundários
    file_contributors = contribution_df.groupby('File Path')['Committer Email'].nunique()
    primary_secondary = []
    
    for file_path, file_group in contribution_df.groupby('File Path'):
        n_contributors = file_contributors[file_path]
        threshold_primary = 100 / n_contributors
        threshold_secondary = 50 / n_contributors
        
        for _, row in file_group.iterrows():
            if row['Share'] >= threshold_primary:
                role = 'primary'
            elif row['Share'] > threshold_secondary:
                role = 'secondary'
            else:
                continue
            
            primary_secondary.append({
                'File Path': file_path,
                'Committer Email': row['Committer Email'],
                'Role': role
            })
    
    role_df = pd.DataFrame(primary_secondary)
    
    # Simular remoção iterativa
    developer_impact = role_df.groupby('Committer Email')['File Path'].nunique()
    sorted_devs = developer_impact.sort_values(ascending=False).index.tolist()
    
    abandoned_files = set()
    bus_factor = 0
    
    for dev in sorted_devs:
        bus_factor += 1
        dev_files = role_df[
            (role_df['Committer Email'] == dev) & 
            (role_df['Role'].isin(['primary', 'secondary']))
        ]['File Path'].tolist()
        
        # Verificar se os arquivos ficam abandonados
        for file_path in dev_files:
            remaining = role_df[
                (role_df['File Path'] == file_path) & 
                (role_df['Committer Email'] != dev)
            ]
            if remaining.empty:
                abandoned_files.add(file_path)
        
        if len(abandoned_files) / changes_files_total > 0.5:
            return bus_factor
    
    return bus_factor

def calc_bus_factor(repository_commits: pd.DataFrame, change_type: str = "large") -> pd.DataFrame:
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

    changes_small = pd.DataFrame()
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


    # ANAL. ============================================================================================================
    changes_files_total: int = 0
    bus_factor_avelino: int = 0
    bus_factor_cosentino: int = 0
    if not changes.empty:
        # Cria uma chave de agrupamento combinando New e Old paths
        changes['File Path'] = changes.apply(
            lambda x: x['Local File PATH New'] if pd.notna(x['Local File PATH New']) 
                    else x['Local File PATH Old'], 
            axis=1
        )
        changes_files_total = len(changes.groupby('File Path'))

        changes['Committer Commit Date'] = changes['Committer Commit Date'].apply(
            lambda x: x[:-3] + x[-2:]  # Remove o ':' do offset (+02:00 → +0200)
        )
        changes['Committer Commit Date'] = changes['Committer Commit Date'].apply(
            lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S%z').astimezone(datetime.timezone.utc)
        )
        changes = changes.sort_values(by='Committer Commit Date')
    
        # Calcular Bus Factor
        bus_factor_avelino = calculate_bus_factor_avelino(changes.copy())
        bus_factor_cosentino = calculate_bus_factor_cosentino(changes.copy())

    changes_large_files_total: int = 0
    bus_factor_avelino_large: int = 0
    bus_factor_cosentino_large: int = 0
    if not changes_large.empty:
        changes_large['File Path'] = changes_large.apply(
            lambda x: x['Local File PATH New'] if pd.notna(x['Local File PATH New']) 
                    else x['Local File PATH Old'], 
            axis=1
        )
        changes_large_files_total = len(changes_large.groupby('File Path'))

        changes_large['Committer Commit Date'] = changes_large['Committer Commit Date'].apply(
            lambda x: x[:-3] + x[-2:]
        )
        changes_large['Committer Commit Date'] = changes_large['Committer Commit Date'].apply(
            lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S%z').astimezone(datetime.timezone.utc)
        )
        changes_large = changes_large.sort_values(by='Committer Commit Date')
    
        # Calcular Bus Factor
        bus_factor_avelino_large = calculate_bus_factor_avelino(changes_large.copy())
        bus_factor_cosentino_large = calculate_bus_factor_cosentino(changes_large.copy())

    changes_small_files_total: int = 0
    bus_factor_avelino_small: int = 0
    bus_factor_cosentino_small: int = 0
    if not changes_small.empty:
        changes_small['File Path'] = changes_small.apply(
            lambda x: x['Local File PATH New'] if pd.notna(x['Local File PATH New']) 
                    else x['Local File PATH Old'], 
            axis=1
        )
        changes_small_files_total = len(changes_small.groupby('File Path'))

        changes_small['Committer Commit Date'] = changes_small['Committer Commit Date'].apply(
            lambda x: x[:-3] + x[-2:]  # Aplicar o mesmo ajuste
        )
        changes_small['Committer Commit Date'] = changes_small['Committer Commit Date'].apply(
            lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S%z').astimezone(datetime.timezone.utc)
        )
        changes_small = changes_small.sort_values(by='Committer Commit Date')
    
        # Calcular Bus Factor
        bus_factor_avelino_small = calculate_bus_factor_avelino(changes_small.copy())
        bus_factor_cosentino_small = calculate_bus_factor_cosentino(changes_small.copy())
    
    # Result ===========================================================================================================
    result: dict = {
        "Type": [change_type],
        "#Files": [added_files_total],
        "Filtered Files Total": [added_files_filtered_total],
        
        "Total Changes Files": [changes_files_total],
        "Bus Factor (Avelino)": [bus_factor_avelino],
        "Bus Factor (Cosentino)": [bus_factor_cosentino],
        
        "Large Files Total": [changes_large_files_total],
        "Bus Factor Large (Avelino)": [bus_factor_avelino_large],
        "Bus Factor Large (Cosentino)": [bus_factor_cosentino_large],
        
        "Small Files Total": [changes_small_files_total],
        "Bus Factor Small (Avelino)": [bus_factor_avelino_small],
        "Bus Factor Small (Cosentino)": [bus_factor_cosentino_small],
    }
    return pd.DataFrame(result)

def process_language(lang: str, large: pd.DataFrame, small: pd.DataFrame, output_path: str):
    """Processa e salva resultados por linguagem"""
    results:list[pd.DataFrame] = []
    if not large.empty:
        results.append(calc_bus_factor(large, 'large'))
    if not small.empty:
        results.append(calc_bus_factor(small, 'small'))
    
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
        project_results.append(calc_bus_factor(large_df))
    if not small_df.empty:
        project_results.append(calc_bus_factor(small_df, 'small'))

    if project_results:
        pd.concat(project_results).to_csv(f"{output_path}/per_project/{repo_path}.csv", index=False)

# Processa última linguagem
if not current_large.empty or not current_small.empty:
    process_language(current_language, current_large, current_small, output_path)

# Resultado global ============================================================================================
final_results: list[pd.DataFrame] = []
if not large_files_commits.empty:
    final_results.append(calc_bus_factor(large_files_commits))
if not small_files_commits.empty:
    final_results.append(calc_bus_factor(small_files_commits, 'small'))

if final_results:
    pd.concat(final_results).to_csv(f"{output_path}/global_results.csv", index=False)
