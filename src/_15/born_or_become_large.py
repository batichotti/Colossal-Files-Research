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
def born_or_become(repository_commits: pd.DataFrame, path: str, change_type: str = "large") -> pd.DataFrame:
    """Classifica os periodos de um arquivo onde ele foi grande"""

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

            # born_large.to_csv(f"{output_path}/{path}/{change_type}s_born.csv", index=False)


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


    # NO LONGER LARGE =============================================================================================
    no_longer_large: pd.DataFrame = pd.DataFrame()

    concat_list = []
    if not born_large.empty:
        concat_list.append(born_large)
    if not modified_large.empty:
        concat_list.append(modified_large)
    if concat_list:
        no_longer_large = repository_commits[repository_commits['Local File PATH New'].isin(pd.concat(concat_list)['Local File PATH New'])].copy()

    # Criar chaves compostas de born_large e modified_large
    combined_keys: pd.DataFrame = pd.DataFrame()
    if concat_list:
        combined_keys = pd.concat(concat_list)[['Local File PATH New', 'Hash']].drop_duplicates()

        # Filtrar no_longer_large para remover linhas que existem em combined_keys
        no_longer_large = no_longer_large.merge(
            combined_keys,
            on=['Local File PATH New', 'Hash'],
            how='left',
            indicator=True
        )

        # Manter apenas as linhas que NÃO estão em combined_keys
        no_longer_large = no_longer_large[no_longer_large['_merge'] == 'left_only'].drop(columns='_merge')

    no_longer_large_grouped_size = 0
    if not no_longer_large.empty:
        no_longer_large = no_longer_large.sort_values(by='Committer Commit Date')
        no_longer_large_grouped = no_longer_large.groupby('Local File PATH New')
        no_longer_large_grouped_size = len(no_longer_large_grouped)

    remaining_no_longer = []
    if no_longer_large_grouped_size:
        for file_path, group in no_longer_large_grouped:
            last_commit_date = group['Committer Commit Date'].min()
            # born
            if born_large.empty and file_path in born_large['Local File PATH New'].values:
                born_last_commit_date = born_large[born_large['Local File PATH New'] == file_path]['Committer Commit Date'].max()
            else:
                born_last_commit_date = str(pd.Timestamp.min)
            # become
            if modified_large_total and file_path in modified_large_per_file.groups:
                become_last_commit_date = modified_large_per_file.get_group(file_path)['Committer Commit Date'].max()
            else:
                become_last_commit_date = str(pd.Timestamp.min)
            #comparação de data
            if last_commit_date > born_last_commit_date and last_commit_date > become_last_commit_date:
                remaining_no_longer.append(group)

    no_longer:pd.DataFrame = pd.DataFrame()
    if remaining_no_longer:
        no_longer = pd.concat(remaining_no_longer)
        # no_longer.to_csv(f"{output_path}/{path}/{change_type}s_no_longer.csv", index=False)


    # FLEX LARGE ========================================================================================================
    flex_large: pd.DataFrame = pd.DataFrame()
    flex_large_total = 0

    concat_list = []
    if not born_large.empty:
        concat_list.append(born_large)
    if not modified_large.empty:
        concat_list.append(modified_large)
    if concat_list:
        flex_large = repository_commits[repository_commits['Local File PATH New'].isin(pd.concat(concat_list)['Local File PATH New'])].copy()

    if not no_longer.empty:
        concat_list.append(no_longer)

    combined_keys: pd.DataFrame = pd.DataFrame()
    if concat_list:
        # Excluir registros onde a combinação Local File PATH New + Hash está em born_large ou modified_large ou no_longer
        combined_keys = pd.concat(concat_list)[['Local File PATH New', 'Hash']].drop_duplicates()

        # Usar merge para identificar registros que NÃO estão em combined_keys
        flex_large = flex_large.merge(
            combined_keys,
            on=['Local File PATH New', 'Hash'],
            how='left',
            indicator=True
        )

        # Manter apenas os registros que não estão em combined_keys
        flex_large = flex_large[flex_large['_merge'] == 'left_only'].drop(columns='_merge')

    if not flex_large.empty:
        flex_large = flex_large.sort_values(by='Committer Commit Date')

        # flex_large.to_csv(f"{output_path}/{path}/{change_type}s_flex.csv", index=False)

        flex_large_grouped = flex_large.groupby('Local File PATH New')
        flex_large_total = len(flex_large_grouped)


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


    # Other pt1 =================================================================================================
    # Excluir registros onde a combinação Local File PATH New + Hash está em born_large ou become_large ou no_longer
    concat_list = []
    if not born_large.empty:
        concat_list.append(born_large)
    if not become_large.empty:
        concat_list.append(become_large)
    if not flex_large.empty:
        concat_list.append(flex_large)
    if not no_longer.empty:
        concat_list.append(no_longer)
    if concat_list:
        combined_keys = pd.concat(concat_list)['Local File PATH New'].drop_duplicates()

        if not modified_large.empty:
            # Usar merge para identificar registros que NÃO estão em combined_keys
            modified_large = modified_large.merge(
                combined_keys,
                on='Local File PATH New',
                how='left',
                indicator=True
            )

            # Manter apenas os registros que não estão em combined_keys
            modified_large = modified_large[modified_large['_merge'] == 'left_only'].drop(columns='_merge')

            if not modified_large.empty:
                modified_large = modified_large.sort_values(by='Committer Commit Date')

                # modified_large.to_csv(f"{output_path}/{path}/{change_type}s_modified.csv", index=False)
                modified_large_grouped = modified_large.groupby('Local File PATH New')
                modified_large_total = len(modified_large_grouped)

    # RESULT ================================================================================================
    result: dict = {
        "Type": [change_type],
        "Added Files TOTAL": [babies_total],
        "Added Large Files TOTAL": [len(born_large)],
        "Added Large Files Percentage": [(len(born_large)/babies_total)*100],
        "Added and Modified Files TOTAL": [added_modified_total],
        "Become Large Files TOTAL": [become_large_total],
        "Become Large Files Percentage": [((become_large_total/added_modified_total)*100) if added_modified_total > 0 else 0],
        "Modified Files TOTAL": [modifieds_total],
        "Modified Large Files TOTAL": [modified_large_total],
        "Modified Large Files Percentage": [((modified_large_total/modifieds_total)*100) if modifieds_total > 0 else 0],
        "Flex Large Files TOTAL": [flex_large_total],
        "Flex Large Files Percentage": [(flex_large_total/files_total)*100],
        "No Longer Large Files TOTAL": [len(remaining_no_longer)],
        "No Longer Large Files Percentage": [(len(remaining_no_longer)/files_total)*100]

    }
    return pd.DataFrame(result)


def process_language(lang: str, large: pd.DataFrame, small: pd.DataFrame, output_path: str):
    """Processa e salva resultados por linguagem"""
    results:list[pd.DataFrame] = []
    if not large.empty:
        results.append(born_or_become(large, f"commits/per_language/{lang}", 'large'))
    if not small.empty:
        results.append(born_or_become(small, f"commits/per_language/{lang}", 'small'))

    if results:
        pd.concat(results).to_csv(f"{output_path}/per_language/{lang}.csv", index=False)

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

    # Processa arquivos pequenos
    small_path = f"{small_files_commits_path}{repo_path}.csv"
    small_df: pd.DataFrame = pd.DataFrame()
    if path.exists(small_path):
        small_df: pd.DataFrame = pd.read_csv(small_path, sep=SEPARATOR)
        current_small = pd.concat([current_small, small_df])
        small_files_commits = pd.concat([small_files_commits, small_df])

    project_results: list[pd.DataFrame] = []
    if not large_df.empty:
        project_results.append(born_or_become(large_df, f"commits/per_project/{repo_path}"))
    if not small_df.empty:
        project_results.append(born_or_become(small_df, f"commits/per_project/{repo_path}", 'small'))

    if project_results:
        pd.concat(project_results).to_csv(f"{output_path}/per_project/{repo_path}.csv", index=False)

# Processa última linguagem
if not current_large.empty or not current_small.empty:
    process_language(current_language, current_large, current_small, output_path)

# Resultado global ============================================================================================
final_results: list[pd.DataFrame] = []
if not large_files_commits.empty:
    final_results.append(born_or_become(large_files_commits, "commits"))
if not small_files_commits.empty:
    final_results.append(born_or_become(small_files_commits, "commits", 'small'))

if final_results:
    pd.concat(final_results).to_csv(f"{output_path}/global_results.csv", index=False)
