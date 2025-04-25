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
language_white_list_path: str = f"./src/_12/input/white_list.csv"
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
def born_or_become(repository_commits: pd.DataFrame, path: str, white_list: pd.DataFrame, change_type: str = "large") -> pd.DataFrame:
    """Classifica os periodos de um arquivo onde ele foi grande"""
    
    white_list_fl = white_list['path'].apply(lambda x: "/".join(x.split("/")[6:])).values
    repository_commits = repository_commits[
        repository_commits['Local File PATH New'].isin(white_list_fl) |
        repository_commits['Local File PATH Old'].isin(white_list_fl) 
    ]

    files_total = len(repository_commits.groupby('Local File PATH New'))


    # BORN ==================================================================================================
    born_large = repository_commits[repository_commits['Change Type'] == 'ADD'].copy()
    babies_total: int = len(born_large)

    # Remove linhas onde 'File Name' não é uma string ou não contém um ponto
    born_large = born_large[born_large['File Name'].apply(lambda x: isinstance(x, str) and '.' in x)]
    if not born_large.empty:
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

        if not born_large.empty:
            born_large = born_large.sort_values(by='Committer Commit Date')


    # MODIFIED ===============================================================================================================
    modified_large = repository_commits[repository_commits['Change Type'] == 'MODIFY'].copy()
    modifieds_total = len(modified_large.groupby('Local File PATH New'))
    modified_large_total = 0
    modified_large_per_file = None

    # Filtrar modified_large para remover linhas que existem em born_large
    if not born_large.empty:
        modified_large = modified_large[~modified_large['Local File PATH New'].isin(born_large['Local File PATH New'].values)]

    # Remove linhas onde 'File Name' não é uma string ou não contém um ponto
    modified_large = modified_large[modified_large['File Name'].apply(lambda x: isinstance(x, str) and '.' in x)]
    if not modified_large.empty:
        modified_large['Extension'] = modified_large['File Name'].apply(lambda x: x.split(".")[-1])
        modified_large = modified_large[modified_large['Extension'].isin(language_white_list_df['Extension'].values)]

        modified_large = modified_large.merge(
            language_white_list_df[['Extension', 'Language']],
            on='Extension',
            how='left'
        ).drop(columns=['Extension'])

        # Converte NLOC e remove inválidos
        modified_large['Lines Of Code (nloc)'] = pd.to_numeric(modified_large['Lines Of Code (nloc)'], errors='coerce')
        modified_large = modified_large.dropna(subset=['Language', 'Lines Of Code (nloc)'])

        # Filtra pelo percentil
        modified_large = modified_large[modified_large.apply(
            lambda x: x['Lines Of Code (nloc)'] >= percentil_99.get(x['Language'], 0),
            axis=1
        )]

        if not modified_large.empty:
            modified_large = modified_large.sort_values(by='Committer Commit Date')
            modified_large_per_file = modified_large.groupby('Local File PATH New')
            modified_large_total = len(modified_large_per_file)

    # BECOME ==================================================================================================================
    become_large = repository_commits[repository_commits['Change Type'] == 'MODIFY'].copy()
    added_files = repository_commits[repository_commits['Change Type'] == 'ADD']

    # Filtrar become_large para remover linhas que temos os commits de adição
    become_large = become_large[become_large['Local File PATH New'].isin(added_files['Local File PATH New'].values)]
    become_large_total = 0
    added_modified_total = 0
    if not become_large.empty:
        added_modified_total = len(become_large.groupby('Local File PATH New'))

        concat_list = []
        if not born_large.empty:
            concat_list.append(born_large)
        if concat_list:
        # Filtrar become_large para remover linhas que n existem em born_large
            become_large = become_large[~become_large['Local File PATH New'].isin(born_large['Local File PATH New'].values)]

        # Remove linhas onde 'File Name' não é uma string ou não contém um ponto
        become_large = become_large[become_large['File Name'].apply(lambda x: isinstance(x, str) and '.' in x)]
        if not become_large.empty:
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

            if not become_large.empty:
                # become_large.to_csv(f"{output_path}/{path}/{change_type}s_become.csv", index=False)
                become_large_grouped = become_large.groupby('Local File PATH New')
                become_large_total = len(become_large_grouped)


    # RESULT ================================================================================================
    result: dict = {
        "Type": [change_type],
        "Added Large Files TOTAL": [len(born_large)],
        "Become Large Files TOTAL": [become_large_total],
    }
    return pd.DataFrame(result)


def process_language(lang: str, large: pd.DataFrame, small: pd.DataFrame, output_path: str):
    """Processa e salva resultados por linguagem"""
    results:list[pd.DataFrame] = []
    if not large.empty:
        results.append(born_or_become(large, f"commits/per_language/{lang}", 'large'))

    if results:
        pd.concat(results).to_csv(f"{output_path}/per_language/{lang}.csv", index=False)

# Processamento principal =====================================================================================
current_language: str = None
current_large: pd.DataFrame = pd.DataFrame()

for i, row in repositories.iterrows():
    repo_url: str = row['url']
    language: str = row['main language']
    repo_name: str = repo_url.split('/')[-1]
    repo_owner: str = repo_url.split('/')[-2]
    repo_path: str = f"{language}/{repo_owner}~{repo_name}"

    print(repo_path)

    # Cria diretórios necessários
    makedirs(f"{output_path}/per_project/{language}/", exist_ok=True)
    makedirs(f"{output_path}/commits/per_project/{repo_path}/", exist_ok=True)
    makedirs(f"{output_path}/per_language/", exist_ok=True)
    makedirs(f"{output_path}/commits/per_language/{language}/", exist_ok=True)

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

# Processa última linguagem
if not current_large.empty or not current_small.empty:
    process_language(current_language, current_large, current_small, output_path)
