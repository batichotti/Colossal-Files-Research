import pandas as pd
from os import makedirs, path
from sys import setrecursionlimit
from datetime import timezone
from dateutil import parser
import numpy as np

setrecursionlimit(2_000_000)

SEPARATOR = '|'

# Setup =======================================================================================================
input_path: str = "./src/_25/input/"
output_path: str = "./src/_25/output/"

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
    """

    # SETUP ===============================================================================================================
    # added_files: pd.DataFrame = repository_commits[repository_commits['Change Type'] == 'ADD'].copy()
    added_files: pd.DataFrame = repository_commits.copy()
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
    
    changes = added_files
    # changes = repository_commits[
    #     repository_commits['Local File PATH New'].isin(added_files['Local File PATH New'].values) |
    #     repository_commits['Local File PATH New'].isin(added_files['Local File PATH Old'].values)
    #     ].copy()

    # Cria um mapeamento completo de TODOS os caminhos (e Old) para linguagem
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

    small:pd.DataFrame = pd.DataFrame()
    changes_large: pd.DataFrame = changes.copy()
    if not changes_large.empty:
        # Converte NLOC para numérico e remove inválidos
        changes_large['Lines Of Code (nloc)'] = pd.to_numeric(changes_large['Lines Of Code (nloc)'], errors='coerce')
        changes_large = changes_large.dropna(subset=['Language', 'Lines Of Code (nloc)'])

        # Filtra as linhas onde a linguagem é igual e o número de linhas de código é menor que o percentil 99
        percentil_99 = percentil_df.set_index('language')['percentil 99']
        small_list = changes_large[changes_large.apply(
            lambda x: x['Lines Of Code (nloc)'] < percentil_99.get(x['Language'], 0),
            axis=1
        )].copy()
        large_list = changes_large[changes_large.apply(
            lambda x: x['Lines Of Code (nloc)'] >= percentil_99.get(x['Language'], 0),
            axis=1
        )]

    changes_together = pd.DataFrame()
    changes_small = pd.DataFrame()
    if not large_list.empty:
        # Identifica os hashes de commits que possuem arquivos grandes
        large_hashes = set(large_list['Hash'])
        # Filtra changes_large para incluir apenas arquivos grandes e excluir hashes com arquivos pequenos
        small_hashes = set(small_list['Hash'])
        # Cria a categoria "together" para casos com arquivos grandes e pequenos juntos
        together_hashes = large_hashes.intersection(small_hashes)
        # Remove a interseção
        large_hashes = large_hashes - together_hashes
        small_hashes = small_hashes - together_hashes

        # Filtra changes_small para excluir arquivos e hashes de commits de large files
        changes_large = changes[changes['Hash'].isin(large_hashes)].copy()
        changes_small = changes[changes['Hash'].isin(small_hashes)].copy()
        changes_together = changes[changes['Hash'].isin(together_hashes)].copy()

    # ANAL ================================================================================================================
    def bus_factor(df: pd.DataFrame):
        # Ordena os commits por data (do mais velho para o mais novo)
        df['Committer Commit Date'] = df.apply(
            lambda x: parser.parse(x['Committer Commit Date']).astimezone(timezone.utc),
            axis=1
        )
        df = df.sort_values(by='Committer Commit Date')

        # Quantos commits cada autor tem, guarde em um dicionário organizado do autor que mais tem commits para o que menos tem
        author_commit_counts = df['Author Email'].value_counts()
        author_commit_counts = author_commit_counts.sort_values(ascending=False)

        # Quantos commits equivalem a 70% de commits
        total_commits: int = df['Hash'].nunique()
        threshold_70: int = round(0.7 * total_commits)

        # Menor número de autores que correspondem a 70% dos commits
        cumulative_commits = 0
        num_top_authors = 0

        for author, commit_count in author_commit_counts.items():
            # Atropelando os autores em ordem descendente
            cumulative_commits += commit_count
            num_top_authors += 1
            if cumulative_commits >= threshold_70:
                break
        return num_top_authors


    commits_amount = 0
    if not changes.empty:
        commits_amount = changes['Hash'].nunique()

    num_top_authors_large = 0
    if not changes_large.empty:
        num_top_authors_large = bus_factor(changes_large)

    num_top_authors_small = 0
    if not changes_small.empty:
        num_top_authors_small = bus_factor(changes_small)

    num_top_authors_together = 0
    if not changes_together.empty:
        num_top_authors_together = bus_factor(changes_together)

    large_files_commit: pd.DataFrame = pd.DataFrame()
    small_files_commit: pd.DataFrame = pd.DataFrame()
    if not changes.empty:
        changes['File Path'] = changes.apply(
            lambda x: x['Local File PATH New'] if pd.notna(x['Local File PATH New'])
                    else x['Local File PATH Old'],
            axis=1
        )

        if not large_list.empty:
            large_paths = pd.concat([
                large_list['Local File PATH New'],
                large_list['Local File PATH Old']
            ])
            small_files_commit = changes[~changes['File Path'].isin(large_paths)].copy()
            large_files_commit = changes[changes['File Path'].isin(large_paths)].copy()

    large_file_bus_factor: list[int] = []
    commits_large_list: list = []
    authors_large_list: list = []
    if not large_files_commit.empty:
        for _, file_group in large_files_commit.groupby('File Path'):
            # if (len(file_group) >= 10):
            large_file_bus_factor.append(bus_factor(file_group))
            commits_large_list.append(len(file_group))
            authors_large_list.append(len(file_group.groupby('Author Email')))

    small_file_bus_factor: list[int] = []
    commits_small_list: list = []
    authors_small_list: list = []
    if not small_files_commit.empty:
        for _, file_group in small_files_commit.groupby('File Path'):
            # if (len(file_group) >= 10):
            small_file_bus_factor.append(bus_factor(file_group))
            commits_small_list.append(len(file_group))
            authors_small_list.append(len(file_group.groupby('Author Email')))

    # Calcula estatísticas para arquivos grandes
    large_total_files = len(commits_large_list)
    large_mean = np.mean(large_file_bus_factor) if large_file_bus_factor else 0
    large_median = np.median(large_file_bus_factor) if large_file_bus_factor else 0
    large_min = np.min(large_file_bus_factor) if large_file_bus_factor else 0
    large_max = np.max(large_file_bus_factor) if large_file_bus_factor else 0

    # Calcula estatísticas para commits de arquivos grandes
    commits_large_mean = np.mean(commits_large_list) if commits_large_list else 0
    commits_large_median = np.median(commits_large_list) if commits_large_list else 0

    # Calcula estatísticas para autores de arquivos grandes
    authors_large_mean = np.mean(authors_large_list) if authors_large_list else 0
    authors_large_median = np.median(authors_large_list) if authors_large_list else 0

    # Calcula estatísticas para arquivos pequenos
    small_total_files = len(commits_small_list)
    small_mean = np.mean(small_file_bus_factor) if small_file_bus_factor else 0
    small_median = np.median(small_file_bus_factor) if small_file_bus_factor else 0
    small_min = np.min(small_file_bus_factor) if small_file_bus_factor else 0
    small_max = np.max(small_file_bus_factor) if small_file_bus_factor else 0

    # Calcula estatísticas para commits de arquivos pequenos
    commits_small_mean = np.mean(commits_small_list) if commits_small_list else 0
    commits_small_median = np.median(commits_small_list) if commits_small_list else 0

    # Calcula estatísticas para autores de arquivos pequenos
    authors_small_mean = np.mean(authors_small_list) if authors_small_list else 0
    authors_small_median = np.median(authors_small_list) if authors_small_list else 0

    # Adicione os resultados à variável result
    result: dict = {
        "Type": [change_type],
        "Commits Amount": [commits_amount],

        "70% Threshould Large": [num_top_authors_large],
        "70% Threshould Small": [num_top_authors_small],
        "70% Threshould Flex": [num_top_authors_together],

        "Large Files": [large_total_files],
        "Large Mean": [large_mean],
        "Large Median": [large_median],
        "Large Min": [large_min],
        "Large Max": [large_max],

        "Commits Large Mean": [commits_large_mean],
        "Commits Large Median": [commits_large_median],

        "Authors Large Mean": [authors_large_mean],
        "Authors Large Median": [authors_large_median],

        "Small Files": [small_total_files],
        "Small Mean": [small_mean],
        "Small Median": [small_median],
        "Small Min": [small_min],
        "Small Max": [small_max],

        "Commits Small Mean": [commits_small_mean],
        "Commits Small Median": [commits_small_median],

        "Authors Small Mean": [authors_small_mean],
        "Authors Small Median": [authors_small_median]
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
