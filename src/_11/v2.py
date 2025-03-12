import pandas as pd
from os import makedirs, scandir, path, listdir
from concurrent.futures import ThreadPoolExecutor

SEPARATOR = '|'

# Setup
input_path:str = "./src/_10/input/"
output_path:str = "./src/_10/output/"

repositories_path:str = "./src/_00/input/450_Starred_Projects.csv"
cloc_path:str = "./src/_01/output/"
large_files_commits_path:str = "./src/_04/output/"
small_files_commits_path:str = "./src/_08/output/"

repositories:pd.DataFrame = pd.read_csv(repositories_path)

def process_repository(i):
    # getting repository information
    repository, language = repositories.loc[i, ['url', 'main language']]

    makedirs(f"{output_path}large_files/{language}", exist_ok=True)
    makedirs(f"{output_path}small_files/{language}", exist_ok=True)
    
    repo_path: str = f"{language}/{repository.split('/')[-2]}~{repository.split('/')[-1]}"
    
    hashs_large, hashs_small = [], []
    
    print(repo_path)

    large_files_commits_df:pd.DataFrame = pd.DataFrame()
    if path.exists(f"{large_files_commits_path}{repo_path}"):
        hashs_large = [folder.name for folder in scandir(f"{large_files_commits_path}{repo_path}") if folder.is_dir()]
        print(f'{repo_path} - Large: {len(hashs_large)}')
        for hash in hashs_large:
            # print(f'{repo_path} - {hash}')
            commit_df: pd.DataFrame = pd.read_csv(f"{large_files_commits_path}{repo_path}/{hash}/commit.csv")
            for file in listdir(f"{large_files_commits_path}{repo_path}/{hash}/files"):
                # print(f'{repo_path} - {hash} - {file}')
                file_df: pd.DataFrame = pd.read_csv(f"{large_files_commits_path}{repo_path}/{hash}/files/{file}")
                print(pd.concat([commit_df, file_df], axis=1))
                input()
                large_files_commits_df = pd.concat([large_files_commits_df, pd.concat([commit_df, file_df], axis=1)])
                print(large_files_commits_df)
                input()
        if large_files_commits_df.empty:
            print(f'\033[33mVazio: {repo_path}\033[m')
        large_files_commits_df.to_csv(f"{output_path}large_files/{repo_path}.csv", index=False)
    else:
        print(f'\033[33mPulou Large: {repo_path}\033[m')

    small_files_commits_df = pd.DataFrame()
    if path.exists(f"{small_files_commits_path}{repo_path}"):
        hashs_small = [folder.name for folder in scandir(f"{small_files_commits_path}{repo_path}") if folder.is_dir()]
        print(f'{repo_path} - Small: {len(hashs_large)}')
        for hash in hashs_small:
            # print(f'{repo_path} - {hash}')
            commit_df: pd.DataFrame = pd.read_csv(f"{small_files_commits_path}{repo_path}/{hash}/commit.csv")
            for file in listdir(f"{small_files_commits_path}{repo_path}/{hash}/files"):
                # print(f'{repo_path} - {hash} - {file}')
                file_df: pd.DataFrame = pd.read_csv(f"{small_files_commits_path}{repo_path}/{hash}/files/{file}")
                small_files_commits_df = pd.concat([small_files_commits_df, pd.concat([commit_df, file_df], axis=1)])
        if small_files_commits_df.empty:
            print(f'\033[33mVazio: {repo_path}\033[m')
        small_files_commits_df.to_csv(f"{output_path}small_files/{repo_path}.csv", index=False)
    else:
        print(f'\033[33mPulou Small: {repo_path}\033[m')

with ThreadPoolExecutor() as executor:
    executor.map(process_repository, range(len(repositories)))
