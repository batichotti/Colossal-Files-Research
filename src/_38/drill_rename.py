import pydriller as dr
import pandas as pd
import os
from datetime import datetime

# setting paths -------------------------------------------------------------------------------------------------------

input_path: str = './src/_38/input/'
output_path: str = './src/_38/output/'

# list with repositories that will analyzed
repositories_list_path: str = 'src/_00/input/450-linux-pytorch.csv'

# files and their renaming
renamed_large_path: str = 'src/_36/output/large/'
renamed_small_path: str = 'src/_36/output/small/'

# base dirs
repositories_base_dir: str = './src/_00/output/'

# Date
date = datetime(2024, 12, 23, 23, 0, 0)

# preparing environment -----------------------------------------------------------------------------------------------

# loading list of repositories
repositories: pd.DataFrame = pd.read_csv(repositories_list_path)

for i in range(len(repositories)):
    # Getting repository information
    main_language: str = repositories['main language'].loc[i]
    owner: str = repositories['owner'].loc[i]
    project: str = repositories['project'].loc[i]
    branch: str = repositories['branch'].loc[i]
    
    repo_path: str = f"{main_language}/{owner}~{project}"
    os.makedirs(f"{output_path}large/{main_language}", exists_ok=True)
    # Generating repository path
    repository_path: str = f'{repositories_base_dir}{main_language}/{owner}~{project}'
    print(f'{repository_path} -> {branch}')

    # Generating the path to the repository's files list
    files_list_path: str = f'{renamed_large_path}{main_language}/{owner}~{project}.csv'
    print(f'* {files_list_path} - {len(files_list_path)} files')

    # Loading files list
    files_list: pd.DataFrame = pd.read_csv(files_list_path, sep=';')
    # print(f' --> {files_list}')

    for j in range(len(files_list)):
        # For each file, generate a path
        file_path: str = files_list['path'].loc[j]
        file_name: str = file_path.split('/')[-1]
        file_path = '/'.join(file_path.split('/')[6:])
        print(f'{file_path} - Mininig...')

        # PyDriller  -----------------------------------------------------------------------------------------------

        dir_path = f'{output_path}{main_language}/{owner}~{project}'
        os.makedirs(dir_path, exist_ok=True)

        repository = dr.Repository(repository_path, only_in_branch=branch, filepath=file_path)

        for commit in repository.traverse_commits():
            try:
                # Setting commit path
                commit_dir = f'{dir_path}/{commit.hash}'
                if os.path.exists(commit_dir):
                    continue
                os.makedirs(commit_dir, exist_ok=True)

                # Analyzing and saving commit information
                df_commit: pd.DataFrame = pd.DataFrame({
                    'Hash': [commit.hash],
                    'Project Name': [commit.project_name],
                    'Local Commit PATH': [commit.project_path],
                    'Merge Commit': [commit.merge],
                    'Message': [commit.msg],
                    'Number of Files': [len(commit.modified_files)],
                    'Author Name': [commit.author.name],
                    'Author Email': [commit.author.email],
                    'Author Commit Date': [commit.author_date],
                    'Author Commit Timezone': [commit.author_timezone],
                    'Committer Name': [commit.committer.name],
                    'Committer Email': [commit.committer.email],
                    'Committer Commit Date': [commit.committer_date],
                    'Committer Timezone': [commit.committer_timezone],
                })
                # df_commit.to_csv(f'{commit_dir}/commit.csv', sep='|', index=False)

                for file in commit.modified_files:
                    # Analyzing and saving each commit's file information
                    df_file: pd.DataFrame = pd.DataFrame({
                        'File Name': [file.filename],
                        'Change Type': [str(file.change_type).split('.')[-1]],
                        'Local File PATH Old': [file.old_path if file.old_path else 'new file'],
                        'Local File PATH New': [file.new_path],
                        'Complexity': [file.complexity if file.complexity else 'not calculated'],
                        'Methods': [len(file.methods)],
                        'Tokens': [file.token_count if file.token_count else 'not calculated'],
                        'Lines Of Code (nloc)': [file.nloc if file.nloc else 'not calculated'],
                        'Lines Added': [file.added_lines],
                        'Lines Deleted': [file.deleted_lines],
                    })
                    pd.concat([df_commit, df_file], axis=1).to_csv(
                        f'{output_path}large/{repo_path}.csv',
                        sep='|',
                        mode='a',
                        index=False,
                        header=not os.path.exists(f'{output_path}large/{repo_path}.csv')
                    )

            except Exception as e:
                input(f'\033[33mError: {e}\033[m')

                # Error dir
                error_dir: str = f'{output_path}errors/'
                os.makedirs(error_dir, exist_ok=True)

                # Adding error to the DataFrame
                df_commit['Error'] = str(e)

                # Saving errors
                df_commit.to_csv(f'{error_dir}errors_{commit.project_name}.csv', mode='a', sep='|', index=False)

        print(f'\033[32m    > Minered - {file_name} : {file_path}\033[m')


# FAZENDO AGORA PARA SMALL FILES

for i in range(len(repositories)):

    # Getting repository information
    main_language: str = repositories['main language'].loc[i]
    owner: str = repositories['owner'].loc[i]
    project: str = repositories['project'].loc[i]
    branch: str = repositories['branch'].loc[i]
    
    repo_path: str = f"{main_language}/{owner}~{project}"
    os.makedirs(f"{output_path}small/{main_language}", exists_ok=True)
    
    # Generating repository path
    repository_path: str = f'{repositories_base_dir}{main_language}/{owner}~{project}'
    print(f'{repository_path} -> {branch}')

    # Generating the path to the repository's files list
    files_list_path: str = f'{renamed_small_path}{main_language}/{owner}~{project}.csv'
    print(f'* {files_list_path} - {len(files_list_path)} files')

    # Loading files list
    files_list: pd.DataFrame = pd.read_csv(files_list_path, sep=';')
    # print(f' --> {files_list}')

    for j in range(len(files_list)):
        # For each file, generate a path
        file_path: str = files_list['path'].loc[j]
        file_name: str = file_path.split('/')[-1]
        file_path = '/'.join(file_path.split('/')[6:])
        print(f'{file_path} - Mininig...')

        # PyDriller  -----------------------------------------------------------------------------------------------

        dir_path = f'{output_path}{main_language}/{owner}~{project}'
        os.makedirs(dir_path, exist_ok=True)

        repository = dr.Repository(repository_path, only_in_branch=branch, filepath=file_path)

        for commit in repository.traverse_commits():
            try:
                # Setting commit path
                commit_dir = f'{dir_path}/{commit.hash}'
                if os.path.exists(commit_dir):
                    continue
                os.makedirs(commit_dir, exist_ok=True)

                # Analyzing and saving commit information
                df_commit: pd.DataFrame = pd.DataFrame({
                    'Hash': [commit.hash],
                    'Project Name': [commit.project_name],
                    'Local Commit PATH': [commit.project_path],
                    'Merge Commit': [commit.merge],
                    'Message': [commit.msg],
                    'Number of Files': [len(commit.modified_files)],
                    'Author Name': [commit.author.name],
                    'Author Email': [commit.author.email],
                    'Author Commit Date': [commit.author_date],
                    'Author Commit Timezone': [commit.author_timezone],
                    'Committer Name': [commit.committer.name],
                    'Committer Email': [commit.committer.email],
                    'Committer Commit Date': [commit.committer_date],
                    'Committer Timezone': [commit.committer_timezone],
                })
                # df_commit.to_csv(f'{commit_dir}/commit.csv', sep='|', index=False)

                for file in commit.modified_files:
                    # Analyzing and saving each commit's file information
                    df_file: pd.DataFrame = pd.DataFrame({
                        'File Name': [file.filename],
                        'Change Type': [str(file.change_type).split('.')[-1]],
                        'Local File PATH Old': [file.old_path if file.old_path else 'new file'],
                        'Local File PATH New': [file.new_path],
                        'Complexity': [file.complexity if file.complexity else 'not calculated'],
                        'Methods': [len(file.methods)],
                        'Tokens': [file.token_count if file.token_count else 'not calculated'],
                        'Lines Of Code (nloc)': [file.nloc if file.nloc else 'not calculated'],
                        'Lines Added': [file.added_lines],
                        'Lines Deleted': [file.deleted_lines],
                    })
                    pd.concat([df_commit, df_file], axis=1).to_csv(
                        f'{output_path}small/{repo_path}.csv',
                        sep='|',
                        mode='a',
                        index=False,
                        header=not os.path.exists(f'{output_path}small/{repo_path}.csv')
                    )

            except Exception as e:
                input(f'\033[33mError: {e}\033[m')

                # Error dir
                error_dir: str = f'{output_path}errors/'
                os.makedirs(error_dir, exist_ok=True)

                # Adding error to the DataFrame
                df_commit['Error'] = str(e)

                # Saving errors
                df_commit.to_csv(f'{error_dir}errors_{commit.project_name}.csv', mode='a', sep='|', index=False)

        print(f'\033[32m    > Minered - {file_name} : {file_path}\033[m')
