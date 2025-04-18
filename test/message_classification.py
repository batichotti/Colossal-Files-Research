import pandas as pd
import numpy as np
import re
from os import makedirs, path
from sys import setrecursionlimit

setrecursionlimit(2_000_000)

SEPARATOR = '|'

# Setup =======================================================================================================
input_path: str = "./src/_23/input/"
output_path: str = "./src/_23/output/"

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
def funcao_base(repository_commits: pd.DataFrame, change_type: str = "large") -> pd.DataFrame:
    """Função para classificar mensagens de commit"""

    def classify_commits(repository_commits: pd.DataFrame) -> pd.DataFrame:
        # GIT OPERATIONS
        git_operations = {
            r"branch": "Commit Operation",
            r"merge": "Commit Operation",
            r"integrate": "Commit Operation",
            r"revert": "Commit Operation",
        }

        # BUILT OPERATIONS
        built_configurations = {
            r"build": "Build Configuration",
        }

        # BUG FIX
        bug_fix = {
            r"bug": "Bug-Fix",
            r"fix": "Bug-Fix",
        }
        bug_deny = {
            r"test case": "Test",
            r"unit test": "Test",
        }

        # RESOURCE
        resource = {
            r"conf": "Resource",
            r"license": "Resource",
            r"legal": "Resource",
        }

        # NEW FEATURE
        new_feature = {
            r"update": "New Feature",
            r"add": "New Feature",
            r"new": "New Feature",
            r"create": "New Feature",
            r"implement feature": "New Feature",
            r"enable": "New Feature",
            r"implement": "New Feature",
            r"improve": "New Feature",
        }

        # TEST
        test = {
            r"test": "Test",
        }

        # REFACTOR
        refactor = {
            r"refactor": "Refactor",
        }

        # DEPRECATE
        deprecate = {
            r"deprecat": "Deprecate",
            r"delete": "Deprecate",
            r"clean ?-?up": "Deprecate",
        }

        auto_name = r"\[bot\]"

        auto_emails = {
            r"\[bot\]": "Auto",
            r"@users.noreply.github.com": "Auto",
            r"actions@github.com": "Auto",
            r"noreply@github.com": "Auto",
        }

        keywords = [git_operations, built_configurations, bug_fix, bug_deny, resource, new_feature, test, refactor, deprecate]

        commits_df = repository_commits.copy()

        commits_df['File Path'] = commits_df.apply(
            lambda x: x['Local File PATH New'] if pd.notna(x['Local File PATH New'])
            else x['Local File PATH Old'],
            axis=1
        )

        commits_df.groupby('Hash')

        # LOGIC

        # Analisar mensagens de commit
        # Analisar o Path dos arquivos
        # Analisar emails dos autores

        commits_classification = {
            "Hash": [],
            "Classification": [],
            "Message": [],
            "Message Classification": [],
            "Path": [],
            "Path Classification": [],
            "Committer E-mail": [],
            "Committer Name": [],
            "Committer Classification": []
        }

        for _, commit in commits_df.iterrows():
            commit_hash = commit['Hash']
            message = commit['Message'].lower()
            committer_email = commit['Committer Email'].lower()
            committer_name = commit['Committer Name'].lower()
            paths = commit['File Path'].lower().split(',')

            message_classification = []
            # Categorizar a mensagem do commit
            for keyword in keywords:
                for pattern, classification in keyword.items():
                    if re.search(pattern, message):
                        message_classification.append(classification)

            # Verificar os paths dos arquivos
            path_classification = "Not Test"
            for path in paths:
                if re.search("test", path):
                    path_classification = "Test"

            # Verificar o committer
            committer_classification = "Human"
            if re.search(auto_name, committer_name):
                committer_classification = "Auto"
            for pattern, classification in auto_emails.items():
                if re.search(pattern, committer_email):
                    committer_classification = "Auto"

            # Classificar o commit
            commit_classification = ""
            if committer_classification == "Auto":
                commit_classification = "Auto"
            elif path_classification == "Test":
                commit_classification = "Test"
            else:
                commit_classification = message_classification[0] if message_classification else "Other"

            # Junta o resultado no dicionário
            commit_data = {
                "Hash": commit_hash,
                "Classification": commit_classification,
                "Message": message,
                "Message Classification": ", ".join(message_classification) if message_classification else "Other",
                "Path": ", ".join(paths),
                "Path Classification": path_classification,
                "Committer E-mail": committer_email,
                "Committer Name": committer_name,
                "Committer Classification": committer_classification
            }
            for key, value in commit_data.items():
                commits_classification[key].append(value)

        # Concatena tudo em um DataFrame
        return pd.DataFrame(commits_classification)
    
    
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
    changes_classified: pd.DataFrame = pd.DataFrame()
    if not changes.empty:
        changes_classified = classify_commits(changes)

    changes_large_classified: pd.DataFrame = pd.DataFrame()
    if not changes_large.empty:
        changes_large_classified = classify_commits(changes_large)

    changes_small_classified: pd.DataFrame = pd.DataFrame()
    if not changes_small.empty:
        changes_small_classified = classify_commits(changes_small)

    # ANAL ====================================================================================================================
    
    bug_fix_percentage = 0
    resource_percentage = 0
    new_feature_percentage = 0
    test_percentage = 0
    refactor_percentage = 0
    deprecate_percentage = 0
    auto_percentage = 0
    commit_operation_percentage = 0
    build_configuration_percentage = 0
    other_percentage = 0
    # Calcula a porcentagem de cada classificação para changes_classified
    if not changes_classified.empty:
        classification_counts = changes_classified['Classification'].value_counts(normalize=True) * 100
        bug_fix_percentage = classification_counts.get('Bug-Fix', 0)
        resource_percentage = classification_counts.get('Resource', 0)
        new_feature_percentage = classification_counts.get('New Feature', 0)
        test_percentage = classification_counts.get('Test', 0)
        refactor_percentage = classification_counts.get('Refactor', 0)
        deprecate_percentage = classification_counts.get('Deprecate', 0)
        auto_percentage = classification_counts.get('Auto', 0)
        commit_operation_percentage = classification_counts.get('Commit Operation', 0)
        build_configuration_percentage = classification_counts.get('Build Configuration', 0)
        other_percentage = classification_counts.get('Other', 0)

    bug_fix_percentage_large = 0
    resource_percentage_large = 0
    new_feature_percentage_large = 0
    test_percentage_large = 0
    refactor_percentage_large = 0
    deprecate_percentage_large = 0
    auto_percentage_large = 0
    commit_operation_percentage_large = 0
    build_configuration_percentage_large = 0
    other_percentage_large = 0
    # Calcula a porcentagem de cada classificação para changes_large_classified
    if not changes_large_classified.empty:
        classification_counts_large = changes_large_classified['Classification'].value_counts(normalize=True) * 100
        bug_fix_percentage_large = classification_counts_large.get('Bug-Fix', 0)
        resource_percentage_large = classification_counts_large.get('Resource', 0)
        new_feature_percentage_large = classification_counts_large.get('New Feature', 0)
        test_percentage_large = classification_counts_large.get('Test', 0)
        refactor_percentage_large = classification_counts_large.get('Refactor', 0)
        deprecate_percentage_large = classification_counts_large.get('Deprecate', 0)
        auto_percentage_large = classification_counts_large.get('Auto', 0)
        commit_operation_percentage_large = classification_counts_large.get('Commit Operation', 0)
        build_configuration_percentage_large = classification_counts_large.get('Build Configuration', 0)
        other_percentage_large = classification_counts_large.get('Other', 0)

    bug_fix_percentage_small = 0
    resource_percentage_small = 0
    new_feature_percentage_small = 0
    test_percentage_small = 0
    refactor_percentage_small = 0
    deprecate_percentage_small = 0
    auto_percentage_small = 0
    commit_operation_percentage_small = 0
    build_configuration_percentage_small = 0
    other_percentage_small = 0
    # Calcula a porcentagem de cada classificação para changes_small_classified
    if not changes_small_classified.empty:
        classification_counts_small = changes_small_classified['Classification'].value_counts(normalize=True) * 100
        bug_fix_percentage_small = classification_counts_small.get('Bug-Fix', 0)
        resource_percentage_small = classification_counts_small.get('Resource', 0)
        new_feature_percentage_small = classification_counts_small.get('New Feature', 0)
        test_percentage_small = classification_counts_small.get('Test', 0)
        refactor_percentage_small = classification_counts_small.get('Refactor', 0)
        deprecate_percentage_small = classification_counts_small.get('Deprecate', 0)
        auto_percentage_small = classification_counts_small.get('Auto', 0)
        commit_operation_percentage_small = classification_counts_small.get('Commit Operation', 0)
        build_configuration_percentage_small = classification_counts_small.get('Build Configuration', 0)
        other_percentage_small = classification_counts_small.get('Other', 0)

    # Result ===========================================================================================================
    result: dict = {
        "Type": [change_type],
        # geral
        "Bug-Fix": [bug_fix_percentage],
        "Resource": [resource_percentage],
        "New Feature": [new_feature_percentage],
        "Test": [test_percentage],
        "Refactor": [refactor_percentage],
        "Deprecate": [deprecate_percentage],
        "Auto": [auto_percentage],
        "Commit Operation": [commit_operation_percentage],
        "Build Configuration": [build_configuration_percentage],
        "Other": [other_percentage],

        # large
        "Bug-Fix Large": [bug_fix_percentage_large],
        "Resource Large": [resource_percentage_large],
        "New Feature Large": [new_feature_percentage_large],
        "Test Large": [test_percentage_large],
        "Refactor Large": [refactor_percentage_large],
        "Deprecate Large": [deprecate_percentage_large],
        "Auto Large": [auto_percentage_large],
        "Commit Operation Large": [commit_operation_percentage_large],
        "Build Configuration Large": [build_configuration_percentage_large],
        "Other Large": [other_percentage_large],

        # small
        "Bug-Fix Small": [bug_fix_percentage_small],
        "Resource Small": [resource_percentage_small],
        "New Feature Small": [new_feature_percentage_small],
        "Test Small": [test_percentage_small],
        "Refactor Small": [refactor_percentage_small],
        "Deprecate Small": [deprecate_percentage_small],
        "Auto Small": [auto_percentage_small],
        "Commit Operation Small": [commit_operation_percentage_small],
        "Build Configuration Small": [build_configuration_percentage_small],
        "Other Small": [other_percentage_small]
    }
    return pd.DataFrame(result)

def process_language(lang: str, large: pd.DataFrame, small: pd.DataFrame, output_path: str):
    """Processa e salva resultados por linguagem"""
    results:list[pd.DataFrame] = []
    if not large.empty:
        results.append(funcao_base(large, 'large'))
    if not small.empty:
        results.append(funcao_base(small, 'small'))

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
        project_results.append(funcao_base(large_df))
    if not small_df.empty:
        project_results.append(funcao_base(small_df, 'small'))

    if project_results:
        pd.concat(project_results).to_csv(f"{output_path}/per_project/{repo_path}.csv", index=False)

# Processa última linguagem
if not current_large.empty or not current_small.empty:
    process_language(current_language, current_large, current_small, output_path)

# Resultado global ============================================================================================
final_results: list[pd.DataFrame] = []
if not large_files_commits.empty:
    final_results.append(funcao_base(large_files_commits))
if not small_files_commits.empty:
    final_results.append(funcao_base(small_files_commits, 'small'))

if final_results:
    pd.concat(final_results).to_csv(f"{output_path}/global_results.csv", index=False)
