import pydriller as dr
import pandas as pd
import os
from datetime import datetime

# start timer
start = datetime.now()

# functions to analyze a commit ---------------------------------------------------------------------------------------

# function that analyze a commit information
def commit_anal(commit) -> pd.DataFrame:
    summary: pd.DataFrame = pd.DataFrame({
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
    return summary

# function that analyze a commit's file information
def file_anal(file) -> pd.DataFrame:
    summary: pd.DataFrame = pd.DataFrame({
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
    return summary

# setting paths -------------------------------------------------------------------------------------------------------

input_path: str = './src/_04/input/'
output_path: str = './src/_04/output/'

# list with repositories that will analyzed
repositories_list_path: str = './src/_00/input/reps.csv'

# base dirs
repositories_base_dir: str = './src/_00/output/'
files_base_path:str = './src/_03/output/'

# preparing environment -----------------------------------------------------------------------------------------------

# loading list of repositories
repositories: pd.DataFrame = pd.read_csv(repositories_list_path)

for i in range(len(repositories)):
    # getting repository information
    main_language:str = repositories['main language'].loc[i]
    owner:str = repositories['owner'].loc[i]
    project:str = repositories['project'].loc[i]
    branch: str = repositories['branch'].loc[i]

    # generating repository path
    repository_path: str = f'{repositories_base_dir}{main_language}/{owner}~{project}'
    print(f'{repository_path} -> {branch}')

    # generating the path to the repository's files list
    files_list_path: str = f'{files_base_path}{main_language}/{owner}~{project}.csv'
    print(f'    > {files_list_path}')

    # loading files list
    files_list: pd.DataFrame = pd.read_csv(files_list_path)

    for j in range(len(files_list)):
        # for each file generating a path
        file_path: str = files_list['path'].loc[j] # ./src/_00/output/Java/batichotti~Clube-Do-Filme/web/buscar.jsp
        file_name: str = file_path.split('/')[-1] # buscar.jsp
        file_path = '/'.join(file_path.split('/')[6:]) # web/buscar.jsp
        print(f'        > {file_name}:{file_path}')

# pydriller -----------------------------------------------------------------------------------------------------------

        dir_path = f'{output_path}{main_language}/{owner}~{project}/{file_name}'
        os.makedirs(dir_path, exist_ok=True)

        repository = dr.Repository(repository_path, only_in_branch=branch, filepath=file_path)

        for commit in repository.traverse_commits():
            try:
                # setting commit path
                commit_dir = f'{dir_path}/commit_{j}/'
                os.makedirs(commit_dir, exist_ok=True)

                # analyzing and saving commit information
                df_commit = commit_anal(commit)
                df_commit.to_csv(f'{commit_dir}/commit.csv')

                # setting commit path
                files_dir = f'{commit_dir}/files/'
                os.makedirs(files_dir, exist_ok=True)

                for file in commit.modified_files:
                    # analyzing and saving each commit's file information
                    df_file = file_anal(file)
                    df_file.to_csv(f'{files_dir}{file.filename}.csv')

            except Exception as e:
                print(f'Error: {e}')
                
                # error dir
                error_dir = './mined_commits/errors/'
                os.makedirs(error_dir, exist_ok=True)

                # saving errors
                df_commit.to_csv(f'{error_dir}errors_{commit.project_name}.csv', mode='a', header=False)

# ---------------------------------------------------------------------------------------------------------------------

# end timer and saving it to csv
end = datetime.now()

total_time = pd.DataFrame({
    'Star' : [start],
    'End' : [end],
    'TOTAL' : [end-start],
})
total_time.to_csv(f'{output_path}/total_time.csv')
print(end-start)
