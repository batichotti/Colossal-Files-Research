import pydriller as dr
import pandas as pd
import os
import sys
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# setting paths -------------------------------------------------------------------------------------------------------

input_path: str = './src/_04/input/'
output_path: str = './src/_04/output/'

# Threads/CPU cores ---------------------------------------------------------------------------------------------------
num_cores = os.cpu_count()
print(os.cpu_count())

# Recursion Limit -----------------------------------------------------------------------------------------------------

sys.setrecursionlimit(5000)

# list with repositories that will analyzed
repositories_list_path: str = './src/_00/input/450-linux-pytorch.csv'

# base dirs
repositories_base_dir: str = './src/_00/output/'
files_base_path: str = './src/_03/output/'

# Date
date = datetime(2024, 12, 24, 17, 0, 0)

# preparing environment -----------------------------------------------------------------------------------------------

# loading list of repositories
repositories: pd.DataFrame = pd.read_csv(repositories_list_path, engine='python')

# function to process each repository
def process_repository(i):
    # Start timer
    start = datetime.now()

    # Getting repository information
    main_language: str = repositories['main language'].loc[i]
    owner: str = repositories['owner'].loc[i]
    project: str = repositories['project'].loc[i]
    branch: str = repositories['branch'].loc[i]

    # Generating repository path
    repository_path: str = f'{repositories_base_dir}{main_language}/{owner}~{project}'
    print(f'{repository_path} -> {branch}')

    # if os.path.exists(f'{output_path}{main_language}/{owner}~{project}'):
    #     return

    # Generating the path to the repository's files list
    files_list_path: str = f'{files_base_path}{main_language}/{owner}~{project}.csv'

    # Loading files list
    files_list: pd.DataFrame = pd.read_csv(files_list_path, sep='|', engine='python')
    print(f'* {files_list_path} - {len(files_list)} files')
    # print(f' --> {files_list}')

    for j in range(len(files_list)):
        # For each file, generate a path
        file_path: str = files_list['path'].loc[j]
        file_name: str = file_path.split('/')[-1]
        file_path = '/'.join(file_path.split('/')[6:])
        print(f' Mininig... {main_language}/{owner}~{project} - {file_path}')

        # PyDriller  -----------------------------------------------------------------------------------------------

        dir_path = f'{output_path}{main_language}/{owner}~{project}'
        os.makedirs(dir_path, exist_ok=True)

        repository = dr.Repository(repository_path, only_in_branch=branch, filepath=file_path)

        for commit in repository.traverse_commits():
            try:
                # Setting commit path
                commit_dir = f'{dir_path}/{commit.hash}'
                if os.path.exists(commit_dir):
                    print(f'\033[1;33m    & Already Mined! {main_language}/{owner}~{project}/{file_name} - {commit.hash}\033[m')
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
                df_commit.to_csv(f'{commit_dir}/commit.csv', sep='|', index=False)

                # Setting file path
                files_dir = f'{commit_dir}/files/'
                os.makedirs(files_dir, exist_ok=True)

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
                    df_file.to_csv(f'{files_dir}{file.filename}.csv', sep='|', index=False)

            except Exception as e:
                print(f'\033[33mError: {e}\033[m')

                # Error dir
                error_dir: str = f'{output_path}errors/'
                os.makedirs(error_dir, exist_ok=True)

                # Adding error to the DataFrame
                df_commit['Error'] = str(e)

                # Saving errors
                df_commit.to_csv(f'{error_dir}errors_{commit.project_name}.csv', mode='a', sep='|', index=False)

        print(f'\033[32m    > Mined! {main_language}/{owner}~{project} - {file_path}\033[m')

    # End timer and save it to csv
    end = datetime.now()
    total_time = pd.DataFrame({
        'Start': [start],
        'End': [end],
        'TOTAL': [end - start],
    })
    total_time.to_csv(f'{output_path}{main_language}/{owner}~{project}/total_time.csv', index=False)
    print(f'\033[42m{main_language}/{owner}~{project} - {end - start}\033[m')

if __name__ == '__main__':
    # with ThreadPoolExecutor(max_workers=num_cores) as executor: #Auto-fit
    with ThreadPoolExecutor(max_workers=num_cores) as executor: #Auto-fit
        executor.map(process_repository, range(len(repositories)))
