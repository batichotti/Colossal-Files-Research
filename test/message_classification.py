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

    # SETUP
    # GIT OPERATIONS
    git_operations = {
        r"branch" : "Commmit Operation",
        r"merge" : "Commit Operation",
        r"integrate" : "Commit Operation",
        r"revert" : "Commit Operation",
    }

    # BUILT OPERATIONS
    built_configurantions = {
        r"build" : "Build Configuration",
    }

    # BUG FIX
    bug_fix = {
        r"bug" : "Bug-Fix",
        r"fix" : "Bug-Fix",
    }
    bug_deny = {
        r"test case" : "Test",
        r"unit test" : "Test",
    }

    # RESOURCE
    resource = {
        r"conf" : "Resource",
        r"license" : "Resource",
        r"legal" : "Resource",
    }

    # NEW FEATURE
    new_feature = {
        r"update" : "New Feature",
        r"add" : "New Feature",
        r"new" : "New Feature",
        r"create" : "New Feature",
        r"add" : "New Feature",
        r"implement feature" : "New Feature",
        r"enable" : "New Feature",
        r"implement" : "New Feature",
        r"improve" : "New Feature",
    }

    # TEST
    test = {
        r"test" : "Test",
    }

    # REFACTOR
    refactor = {
        r"refactor" : "Refactor",
    }

    # DEPRECATE
    deprecate = {
        r"deprecat" : "Deprecate",
        r"delete" : "Deprecate",
        r"clean ?-?up" : "Deprecate",
    }

    auto_name = r"\[bot\]"

    auto_emails = {
        r"\[bot\]": "Auto",
        r"@users.noreply.github.com" : "Auto",
        r"actions@github.com" : "Auto",
        r"noreply@github.com" : "Auto",
    }

    keywords = [git_operations, built_configurantions, bug_fix, bug_deny, resource, new_feature, test, refactor, deprecate]

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

    commit_classification = {
        "Hash": [],
        "Classification": [],
        "Message": [],
        "Message Classification": [],
        "Path": [],
        "Path Classification"
        "Committer E-mail": [],
        "Committer Name": [],
        "Committer Classification": []
    }

    for commit in commits_df:
        message = commit['Message'].iloc[0].lower()
        committer_email = commit['Committer Email'].iloc[0].lower()
        committer_name = commit['Committer Email'].iloc[0].lower()
        paths = commit['File Path'].lower().tolist()

        message_classification = []
        # Categorizar a mensagem do commit
        for keyword in keywords:
            for pattern, classification in keyword.items():
                if re.search(pattern, message):
                    message_classification.append(classification)

        # Verificar os paths dos arquivos
        path_classification = "Not Test"
        for path in paths:
            if re.search("test"):
                path_classification = "Test"

        # Verificar o commiter
        committer_classification = "Human"
        if re.search(auto_name, committer_name):
            committer_classification = "Auto"
        for pattern, classification in auto_emails.items():
            if re.search(pattern, committer_email):
                committer_classification = "Auto"


    # Result ===========================================================================================================
    result: dict = {
        "Type": [change_type]
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
