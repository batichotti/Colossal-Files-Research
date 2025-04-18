import pandas as pd
import numpy as np
import statsmodels.api as sm
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
def anal_classification(repository_commits: pd.DataFrame, change_type: str = "large") -> pd.DataFrame:
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
        source_files = {
            r"src" : "Source",
            r"source" : "Source"
        }

        # FEATURE
        feature = {
            r"update": "Feature",
            r"add": "Feature",
            r"new": "Feature",
            r"create": "Feature",
            r"implement feature": "Feature",
        }
        feature_others = {
            r"enable": "Feature",
            r"add": "Feature",
            r"update": "Feature",
            r"implement": "Feature",
            r"improve": "Feature"
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

        auto_name = r"bot"

        auto_emails = {
            r"bot": "Auto",
            r"@users.noreply.github.com": "Auto",
            r"actions@github.com": "Auto",
            r"noreply@github.com": "Auto",
        }

        commits_df = repository_commits.copy()

        commits_classification = {
            "Hash": [],
            "Classification": [],
            "Message": [],
            "Message Classification": [],
            "Path": [],
            "Committer E-mail": [],
            "Committer Name": [],
            "Committer Classification": []
        }

        if not commits_df.empty:
            commits_df['File Path'] = commits_df.apply(
                lambda x: x['Local File PATH New'] if pd.notna(x['Local File PATH New'])
                else x['Local File PATH Old'],
                axis=1
            )

            commits_by_hash = commits_df.groupby('Hash')
            # LOGIC

            # Analisar mensagens de commit
            # Analisar o Path dos arquivos
            # Analisar emails dos autores


            for _, commit in commits_by_hash:
                commit_hash = commit['Hash'].iloc[0]
                message = str(commit['Message'].iloc[0]).lower()
                committer_email = str(commit['Committer Email'].iloc[0]).lower()
                committer_name = str(commit['Committer Name'].iloc[0]).lower()
                paths = commit['File Path'].tolist()

                message_classification = []
                # Categorizar a mensagem do commit =================================================================
                # git operations
                for pattern, classification in git_operations.items():
                    if re.search(pattern, message):
                        message_classification.append(classification)
                        break
                # build congiguration
                for pattern, classification in built_configurations.items():
                    if re.search(pattern, message):
                        message_classification.append(classification)
                        break
                # bug fix
                for pattern, classification in bug_fix.items():
                    if re.search(pattern, message):
                        flag = True
                        for pattern2, classification2 in bug_deny.items():
                            if re.search(pattern2, message):
                                flag = False
                        if flag:
                            message_classification.append(classification)
                            break
                # resource
                is_resource_in_message = False
                for pattern, classification in resource.items():
                    if re.search(pattern, message):
                        is_resource_in_message = True
                        message_classification.append(classification)
                        break
                if not is_resource_in_message:
                    flag = True
                    for pattern, classification in source_files.items():
                        for path in paths:
                            if re.search(pattern, str(path).lower()):
                                flag = False
                    if flag:
                        for pattern, classification in test.items():
                            for path in paths:
                                if re.search(pattern, str(path).lower()):
                                    flag = False
                    if flag:
                        message_classification.append('Resource')
                # feature
                for pattern, classification in feature.items():
                    if re.search(pattern, message):
                        message_classification.append(classification)
                        break
                # test
                is_test_in_message = False
                for pattern, classification in feature.items():
                    if re.search(pattern, message):
                        is_test_in_message = True
                        message_classification.append(classification)
                        break
                if not is_test_in_message:
                    flag = True
                    for pattern, classification in test.items():
                        for path in paths:
                            if not re.search(pattern, str(path).lower()):
                                for pattern2, classification2 in source_files.items():
                                    if re.search(pattern2, str(path).lower()):
                                        flag = False
                                        break
                            if not flag:
                                break
                    if flag:
                        message_classification.append('Test')
                # refactor
                for pattern, classification in refactor.items():
                    if re.search(pattern, message):
                        message_classification.append(classification)
                        break
                # deprecate
                for pattern, classification in deprecate.items():
                    if re.search(pattern, message):
                        message_classification.append(classification)
                        break
                # auto
                committer_classification = "Human"
                if re.search(auto_name, committer_name):
                    committer_classification = "Auto"
                for pattern, classification in auto_emails.items():
                    if re.search(pattern, committer_email):
                        committer_classification = "Auto"
                        break
                # feature others
                if not message_classification:
                    for pattern, classification in feature_others.items():
                        if re.search(pattern, message):
                            message_classification.append(classification)
                            break

                # Classificar o commit ===================================================================================
                commit_classification = ""
                if message_classification:
                    commit_classification = message_classification[0]
                elif committer_classification == "Auto":
                    commit_classification = "Auto"
                else:
                    commit_classification = "Other"

                # Junta o resultado no dicionário =======================================================================
                commit_data = {
                    "Hash": commit_hash,
                    "Classification": commit_classification,
                    "Message": message,
                    "Message Classification": ", ".join(message_classification) if message_classification else "Other",
                    "Path": ", ".join(paths),
                    "Committer E-mail": committer_email,
                    "Committer Name": committer_name,
                    "Committer Classification": committer_classification
                }
                for key, value in commit_data.items():
                    commits_classification[key].append(value)

        # Concatena tudo em um DataFrame =====================================================================================
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
        small = changes_large[changes_large.apply(
            lambda x: x['Lines Of Code (nloc)'] < percentil_99.get(x['Language'], 0),
            axis=1
        )].copy()
        changes_large = changes_large[changes_large.apply(
            lambda x: x['Lines Of Code (nloc)'] >= percentil_99.get(x['Language'], 0),
            axis=1
        )]

    changes_together = pd.DataFrame()
    changes_small = pd.DataFrame()
    if not changes_large.empty:
        # Identifica os hashes de commits que possuem arquivos grandes
        large_hashes = set(changes_large['Hash'])
        # Filtra changes_large para incluir apenas arquivos grandes e excluir hashes com arquivos pequenos
        small_hashes = set(small['Hash'])
        # Cria a categoria "together" para casos com arquivos grandes e pequenos juntos
        together_hashes = large_hashes.intersection(small_hashes)
        # Remove a interseção
        large_hashes = large_hashes - together_hashes
        small_hashes = small_hashes - together_hashes

        # Filtra changes_small para excluir arquivos e hashes de commits de large files
        changes_large = changes[changes['Hash'].isin(large_hashes)].copy()
        changes_small = changes[changes['Hash'].isin(small_hashes)].copy()
        changes_together = changes[changes['Hash'].isin(together_hashes)].copy()


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

    changes_together_classified: pd.DataFrame = pd.DataFrame()
    if not changes_together.empty:
        changes_together_classified = classify_commits(changes_together)

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

    bug_fix_count = 0
    resource_count = 0
    new_feature_count = 0
    test_count = 0
    refactor_count = 0
    deprecate_count = 0
    auto_count = 0
    commit_operation_count = 0
    build_configuration_count = 0
    other_count = 0

    # Calcula a porcentagem e quantidade de cada classificação para changes_classified
    if not changes_classified.empty:
        classification_counts = changes_classified['Classification'].value_counts(normalize=True) * 100
        classification_totals = changes_classified['Classification'].value_counts()
        bug_fix_percentage = classification_counts.get('Bug-Fix', 0)
        resource_percentage = classification_counts.get('Resource', 0)
        new_feature_percentage = classification_counts.get('Feature', 0)
        test_percentage = classification_counts.get('Test', 0)
        refactor_percentage = classification_counts.get('Refactor', 0)
        deprecate_percentage = classification_counts.get('Deprecate', 0)
        auto_percentage = classification_counts.get('Auto', 0)
        commit_operation_percentage = classification_counts.get('Commit Operation', 0)
        build_configuration_percentage = classification_counts.get('Build Configuration', 0)
        other_percentage = classification_counts.get('Other', 0)

        bug_fix_count = classification_totals.get('Bug-Fix', 0)
        resource_count = classification_totals.get('Resource', 0)
        new_feature_count = classification_totals.get('Feature', 0)
        test_count = classification_totals.get('Test', 0)
        refactor_count = classification_totals.get('Refactor', 0)
        deprecate_count = classification_totals.get('Deprecate', 0)
        auto_count = classification_totals.get('Auto', 0)
        commit_operation_count = classification_totals.get('Commit Operation', 0)
        build_configuration_count = classification_totals.get('Build Configuration', 0)
        other_count = classification_totals.get('Other', 0)

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

    bug_fix_count_large = 0
    resource_count_large = 0
    new_feature_count_large = 0
    test_count_large = 0
    refactor_count_large = 0
    deprecate_count_large = 0
    auto_count_large = 0
    commit_operation_count_large = 0
    build_configuration_count_large = 0
    other_count_large = 0

    # Calcula a porcentagem e quantidade de cada classificação para changes_large_classified
    if not changes_large_classified.empty:
        classification_counts_large = changes_large_classified['Classification'].value_counts(normalize=True) * 100
        classification_totals_large = changes_large_classified['Classification'].value_counts()
        bug_fix_percentage_large = classification_counts_large.get('Bug-Fix', 0)
        resource_percentage_large = classification_counts_large.get('Resource', 0)
        new_feature_percentage_large = classification_counts_large.get('Feature', 0)
        test_percentage_large = classification_counts_large.get('Test', 0)
        refactor_percentage_large = classification_counts_large.get('Refactor', 0)
        deprecate_percentage_large = classification_counts_large.get('Deprecate', 0)
        auto_percentage_large = classification_counts_large.get('Auto', 0)
        commit_operation_percentage_large = classification_counts_large.get('Commit Operation', 0)
        build_configuration_percentage_large = classification_counts_large.get('Build Configuration', 0)
        other_percentage_large = classification_counts_large.get('Other', 0)

        bug_fix_count_large = classification_totals_large.get('Bug-Fix', 0)
        resource_count_large = classification_totals_large.get('Resource', 0)
        new_feature_count_large = classification_totals_large.get('Feature', 0)
        test_count_large = classification_totals_large.get('Test', 0)
        refactor_count_large = classification_totals_large.get('Refactor', 0)
        deprecate_count_large = classification_totals_large.get('Deprecate', 0)
        auto_count_large = classification_totals_large.get('Auto', 0)
        commit_operation_count_large = classification_totals_large.get('Commit Operation', 0)
        build_configuration_count_large = classification_totals_large.get('Build Configuration', 0)
        other_count_large = classification_totals_large.get('Other', 0)

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

    bug_fix_count_small = 0
    resource_count_small = 0
    new_feature_count_small = 0
    test_count_small = 0
    refactor_count_small = 0
    deprecate_count_small = 0
    auto_count_small = 0
    commit_operation_count_small = 0
    build_configuration_count_small = 0
    other_count_small = 0

    # Calcula a porcentagem e quantidade de cada classificação para changes_small_classified
    if not changes_small_classified.empty:
        classification_counts_small = changes_small_classified['Classification'].value_counts(normalize=True) * 100
        classification_totals_small = changes_small_classified['Classification'].value_counts()
        bug_fix_percentage_small = classification_counts_small.get('Bug-Fix', 0)
        resource_percentage_small = classification_counts_small.get('Resource', 0)
        new_feature_percentage_small = classification_counts_small.get('Feature', 0)
        test_percentage_small = classification_counts_small.get('Test', 0)
        refactor_percentage_small = classification_counts_small.get('Refactor', 0)
        deprecate_percentage_small = classification_counts_small.get('Deprecate', 0)
        auto_percentage_small = classification_counts_small.get('Auto', 0)
        commit_operation_percentage_small = classification_counts_small.get('Commit Operation', 0)
        build_configuration_percentage_small = classification_counts_small.get('Build Configuration', 0)
        other_percentage_small = classification_counts_small.get('Other', 0)

        bug_fix_count_small = classification_totals_small.get('Bug-Fix', 0)
        resource_count_small = classification_totals_small.get('Resource', 0)
        new_feature_count_small = classification_totals_small.get('Feature', 0)
        test_count_small = classification_totals_small.get('Test', 0)
        refactor_count_small = classification_totals_small.get('Refactor', 0)
        deprecate_count_small = classification_totals_small.get('Deprecate', 0)
        auto_count_small = classification_totals_small.get('Auto', 0)
        commit_operation_count_small = classification_totals_small.get('Commit Operation', 0)
        build_configuration_count_small = classification_totals_small.get('Build Configuration', 0)
        other_count_small = classification_totals_small.get('Other', 0)

    bug_fix_percentage_together = 0
    resource_percentage_together = 0
    new_feature_percentage_together = 0
    test_percentage_together = 0
    refactor_percentage_together = 0
    deprecate_percentage_together = 0
    auto_percentage_together = 0
    commit_operation_percentage_together = 0
    build_configuration_percentage_together = 0
    other_percentage_together = 0

    bug_fix_count_together = 0
    resource_count_together = 0
    new_feature_count_together = 0
    test_count_together = 0
    refactor_count_together = 0
    deprecate_count_together = 0
    auto_count_together = 0
    commit_operation_count_together = 0
    build_configuration_count_together = 0
    other_count_together = 0

    # Calcula a porcentagem e quantidade de cada classificação para changes_together_classified
    if not changes_together_classified.empty:
        classification_counts_together = changes_together_classified['Classification'].value_counts(normalize=True) * 100
        classification_totals_together = changes_together_classified['Classification'].value_counts()
        bug_fix_percentage_together = classification_counts_together.get('Bug-Fix', 0)
        resource_percentage_together = classification_counts_together.get('Resource', 0)
        new_feature_percentage_together = classification_counts_together.get('Feature', 0)
        test_percentage_together = classification_counts_together.get('Test', 0)
        refactor_percentage_together = classification_counts_together.get('Refactor', 0)
        deprecate_percentage_together = classification_counts_together.get('Deprecate', 0)
        auto_percentage_together = classification_counts_together.get('Auto', 0)
        commit_operation_percentage_together = classification_counts_together.get('Commit Operation', 0)
        build_configuration_percentage_together = classification_counts_together.get('Build Configuration', 0)
        other_percentage_together = classification_counts_together.get('Other', 0)

        bug_fix_count_together = classification_totals_together.get('Bug-Fix', 0)
        resource_count_together = classification_totals_together.get('Resource', 0)
        new_feature_count_together = classification_totals_together.get('Feature', 0)
        test_count_together = classification_totals_together.get('Test', 0)
        refactor_count_together = classification_totals_together.get('Refactor', 0)
        deprecate_count_together = classification_totals_together.get('Deprecate', 0)
        auto_count_together = classification_totals_together.get('Auto', 0)
        commit_operation_count_together = classification_totals_together.get('Commit Operation', 0)
        build_configuration_count_together = classification_totals_together.get('Build Configuration', 0)
        other_count_together = classification_totals_together.get('Other', 0)

    # Calcula o resíduo de Pearson qui-quadrado entre a quantidade de cada classificação entre large e small

    # Tabela de contagem de classificações
    classification_counts = np.array([
        [bug_fix_count_large, resource_count_large, new_feature_count_large, test_count_large, refactor_count_large, deprecate_count_large, auto_count_large, commit_operation_count_large, build_configuration_count_large, other_count_large],
        [bug_fix_count_small, resource_count_small, new_feature_count_small, test_count_small, refactor_count_small, deprecate_count_small, auto_count_small, commit_operation_count_small, build_configuration_count_small, other_count_small]
    ])

    table = sm.stats.Table(classification_counts)
    resid_pearson = table.resid_pearson
    standardized_resids = table.standardized_resids

    # Result ===========================================================================================================
    result: dict = {
        "Type": [change_type],

        # geral
        "Bug-Fix": [bug_fix_percentage],
        "Resource": [resource_percentage],
        "Feature": [new_feature_percentage],
        "Test": [test_percentage],
        "Refactor": [refactor_percentage],
        "Deprecate": [deprecate_percentage],
        "Auto": [auto_percentage],
        "Commit Operation": [commit_operation_percentage],
        "Build Configuration": [build_configuration_percentage],
        "Other": [other_percentage],

        "Bug-Fix Count": [bug_fix_count],
        "Resource Count": [resource_count],
        "Feature Count": [new_feature_count],
        "Test Count": [test_count],
        "Refactor Count": [refactor_count],
        "Deprecate Count": [deprecate_count],
        "Auto Count": [auto_count],
        "Commit Operation Count": [commit_operation_count],
        "Build Configuration Count": [build_configuration_count],
        "Other Count": [other_count],

        # large
        "Bug-Fix Large": [bug_fix_percentage_large],
        "Resource Large": [resource_percentage_large],
        "Feature Large": [new_feature_percentage_large],
        "Test Large": [test_percentage_large],
        "Refactor Large": [refactor_percentage_large],
        "Deprecate Large": [deprecate_percentage_large],
        "Auto Large": [auto_percentage_large],
        "Commit Operation Large": [commit_operation_percentage_large],
        "Build Configuration Large": [build_configuration_percentage_large],
        "Other Large": [other_percentage_large],

        "Bug-Fix Large Count": [bug_fix_count_large],
        "Resource Large Count": [resource_count_large],
        "Feature Large Count": [new_feature_count_large],
        "Test Large Count": [test_count_large],
        "Refactor Large Count": [refactor_count_large],
        "Deprecate Large Count": [deprecate_count_large],
        "Auto Large Count": [auto_count_large],
        "Commit Operation Large Count": [commit_operation_count_large],
        "Build Configuration Large Count": [build_configuration_count_large],
        "Other Large Count": [other_count_large],

        # small
        "Bug-Fix Small": [bug_fix_percentage_small],
        "Resource Small": [resource_percentage_small],
        "Feature Small": [new_feature_percentage_small],
        "Test Small": [test_percentage_small],
        "Refactor Small": [refactor_percentage_small],
        "Deprecate Small": [deprecate_percentage_small],
        "Auto Small": [auto_percentage_small],
        "Commit Operation Small": [commit_operation_percentage_small],
        "Build Configuration Small": [build_configuration_percentage_small],
        "Other Small": [other_percentage_small],

        "Bug-Fix Small Count": [bug_fix_count_small],
        "Resource Small Count": [resource_count_small],
        "Feature Small Count": [new_feature_count_small],
        "Test Small Count": [test_count_small],
        "Refactor Small Count": [refactor_count_small],
        "Deprecate Small Count": [deprecate_count_small],
        "Auto Small Count": [auto_count_small],
        "Commit Operation Small Count": [commit_operation_count_small],
        "Build Configuration Small Count": [build_configuration_count_small],
        "Other Small Count": [other_count_small],

        #together
        "Bug-Fix together": [bug_fix_percentage_together],
        "Resource together": [resource_percentage_together],
        "Feature together": [new_feature_percentage_together],
        "Test together": [test_percentage_together],
        "Refactor together": [refactor_percentage_together],
        "Deprecate together": [deprecate_percentage_together],
        "Auto together": [auto_percentage_together],
        "Commit Operation together": [commit_operation_percentage_together],
        "Build Configuration together": [build_configuration_percentage_together],
        "Other together": [other_percentage_together],

        "Bug-Fix together Count": [bug_fix_count_together],
        "Resource together Count": [resource_count_together],
        "Feature together Count": [new_feature_count_together],
        "Test together Count": [test_count_together],
        "Refactor together Count": [refactor_count_together],
        "Deprecate together Count": [deprecate_count_together],
        "Auto together Count": [auto_count_together],
        "Commit Operation together Count": [commit_operation_count_together],
        "Build Configuration together Count": [build_configuration_count_together],
        "Other together Count": [other_count_together],

        # Pearson Resids
        "Pearson Resid - Bug-Fix": resid_pearson[0, 0],
        "Pearson Resid - Resource": resid_pearson[0, 1],
        "Pearson Resid - Feature": resid_pearson[0, 2],
        "Pearson Resid - Test": resid_pearson[0, 3],
        "Pearson Resid - Refactor": resid_pearson[0, 4],
        "Pearson Resid - Deprecate": resid_pearson[0, 5],
        "Pearson Resid - Auto": resid_pearson[0, 6],
        "Pearson Resid - Commit Operation": resid_pearson[0, 7],
        "Pearson Resid - Build Configuration": resid_pearson[0, 8],
        "Pearson Resid - Other": resid_pearson[0, 9],

        #Standardized Resids
        "Standardized Resid - Bug-Fix": standardized_resids[0, 0],
        "Standardized Resid - Resource": standardized_resids[0, 1],
        "Standardized Resid - Feature": standardized_resids[0, 2],
        "Standardized Resid - Test": standardized_resids[0, 3],
        "Standardized Resid - Refactor": standardized_resids[0, 4],
        "Standardized Resid - Deprecate": standardized_resids[0, 5],
        "Standardized Resid - Auto": standardized_resids[0, 6],
        "Standardized Resid - Commit Operation": standardized_resids[0, 7],
        "Standardized Resid - Build Configuration": standardized_resids[0, 8],
        "Standardized Resid - Other": standardized_resids[0, 9]
    }
    return pd.DataFrame(result)

def process_language(lang: str, large: pd.DataFrame, small: pd.DataFrame, output_path: str):
    """Processa e salva resultados por linguagem"""
    results:list[pd.DataFrame] = []
    if not large.empty:
        results.append(anal_classification(large, 'large'))
    if not small.empty:
        results.append(anal_classification(small, 'small'))

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
        project_results.append(anal_classification(large_df))
    if not small_df.empty:
        project_results.append(anal_classification(small_df, 'small'))

    if project_results:
        pd.concat(project_results).to_csv(f"{output_path}/per_project/{repo_path}.csv", index=False)

# Processa última linguagem
if not current_large.empty or not current_small.empty:
    process_language(current_language, current_large, current_small, output_path)

# Resultado global ============================================================================================
final_results: list[pd.DataFrame] = []
if not large_files_commits.empty:
    final_results.append(anal_classification(large_files_commits))
if not small_files_commits.empty:
    final_results.append(anal_classification(small_files_commits, 'small'))

if final_results:
    pd.concat(final_results).to_csv(f"{output_path}/global_results.csv", index=False)
