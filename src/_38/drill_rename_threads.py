import pydriller as dr
import pandas as pd
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# paths e configs
output_path = './src/_38/output/'
repositories_list_path = 'src/_00/input/swift+objectivec.csv'
renamed_large_path = 'src/_36/output/large/'
renamed_small_path = 'src/_36/output/small/'
repositories_base_dir = './src/_00/output/'

# lendo repositorios
repositories = pd.read_csv(repositories_list_path)

def process_repository(i, size='large'):
    main_language = repositories['main language'].loc[i]
    owner = repositories['owner'].loc[i]
    project = repositories['project'].loc[i]
    branch = repositories['branch'].loc[i]
    repo_path = f"{main_language}/{owner}~{project}"
    repository_path = f'{repositories_base_dir}{repo_path}'

    if size == 'large':
        files_list_path = f'{renamed_large_path}{repo_path}.csv'
        out_dir = f'{output_path}large/{repo_path}'
        error_dir = f'{output_path}large/errors/'
    else:
        files_list_path = f'{renamed_small_path}{repo_path}.csv'
        out_dir = f'{output_path}small/{repo_path}'
        error_dir = f'{output_path}small/errors/'

    os.makedirs(os.path.dirname(out_dir), exist_ok=True)

    if not os.path.exists(files_list_path):
        return

    files_list = pd.read_csv(files_list_path, sep=',')
    print(f'* {files_list_path} - {len(files_list)} files')

    for j in range(len(files_list)):
        file_path = files_list['path'].loc[j]
        file_name = file_path.split('/')[-1]
        file_path = '/'.join(file_path.split('/')[6:])
        print(f'{size.upper()} - {repo_path} - {file_path} - Mining...')

        repository = dr.Repository(repository_path, only_in_branch=branch, filepath=file_path)

        for commit in repository.traverse_commits():
            try:
                commit_dir = f'{out_dir}/{commit.hash}'
                if os.path.exists(commit_dir):
                    continue
                os.makedirs(commit_dir, exist_ok=True)

                df_commit = pd.DataFrame({
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
                df_commit.to_csv(f'{commit_dir}/commit.csv', sep=';', index=False)

                files_dir = f'{commit_dir}/files/'
                os.makedirs(files_dir, exist_ok=True)

                for file in commit.modified_files:
                    df_file = pd.DataFrame({
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
                    df_file.to_csv(f'{files_dir}{file.filename}.csv', sep=';', index=False)

            except Exception as e:
                print(f'\033[33mError: {e}\033[m')
                os.makedirs(error_dir, exist_ok=True)
                df_commit['Error'] = str(e)
                df_commit.to_csv(f'{error_dir}errors_{commit.project_name}.csv', mode='a', sep=';', index=False)

        print(f'\033[32m    > MINED {file_name} - {file_path}\033[m')

if __name__ == '__main__':
    num_threads = os.cpu_count()

    print(f'Usando {num_threads} threads para arquivos LARGE...')
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        executor.map(lambda i: process_repository(i, size='large'), range(len(repositories)))

    print(f'Usando {num_threads} threads para arquivos SMALL...')
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        executor.map(lambda i: process_repository(i, size='small'), range(len(repositories)))
