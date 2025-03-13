import pandas as pd
from os import makedirs, scandir, path, listdir
from concurrent.futures import ThreadPoolExecutor

SEPARATOR = '|'

# Setup
input_path:str = "./src/_11/input/"
output_path:str = "./src/_11/output/"

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
            # Lê o commit.csv
            commit_df = pd.read_csv(f"{large_files_commits_path}{repo_path}/{hash}/commit.csv")
            # Lista todos os arquivos dentro da pasta files
            file_dfs = []
            for file in listdir(f"{large_files_commits_path}{repo_path}/{hash}/files"):
                file_path = f"{large_files_commits_path}{repo_path}/{hash}/files/{file}"
                file_df = pd.read_csv(file_path)
                file_dfs.append(file_df)
            # Se houver arquivos, junta eles verticalmente
            if file_dfs:
                df_vertical = pd.concat(file_dfs, ignore_index=True)
                # Adiciona commit.csv horizontalmente para todas as linhas
                if len(commit_df) == 1:
                    commit_df = commit_df.loc[commit_df.index.repeat(len(df_vertical))].reset_index(drop=True)
                df_final = pd.concat([df_vertical, commit_df], axis=1)
                # Adiciona ao DataFrame principal
                large_files_commits_df = pd.concat([large_files_commits_df, df_final], ignore_index=True)
        # Se o DataFrame final não estiver vazio, salva
        if large_files_commits_df.empty:
            print(f'\033[33mVazio Large: {repo_path}\033[m')
        else:
            print(f'\033[32mJuntou Large: {repo_path}\033[m')
            large_files_commits_df.to_csv(f"{output_path}large_files/{repo_path}.csv", index=False)
    else:
        print(f'\033[33mPulou Large: {repo_path}\033[m')

    small_files_commits_df = pd.DataFrame()
    if path.exists(f"{small_files_commits_path}{repo_path}"):
        hashs_small = [folder.name for folder in scandir(f"{small_files_commits_path}{repo_path}") if folder.is_dir()]
        print(f'{repo_path} - Small: {len(hashs_small)}')
        for hash in hashs_small:
            # Lê o commit.csv
            commit_df = pd.read_csv(f"{small_files_commits_path}{repo_path}/{hash}/commit.csv")
            # Lista todos os arquivos dentro da pasta files
            file_dfs = []
            for file in listdir(f"{small_files_commits_path}{repo_path}/{hash}/files"):
                file_path = f"{small_files_commits_path}{repo_path}/{hash}/files/{file}"
                file_df = pd.read_csv(file_path)
                file_dfs.append(file_df)
            # Se houver arquivos, junta eles verticalmente
            if file_dfs:
                df_vertical = pd.concat(file_dfs, ignore_index=True)
                # Adiciona commit.csv horizontalmente para todas as linhas
                if len(commit_df) == 1:
                    commit_df = commit_df.loc[commit_df.index.repeat(len(df_vertical))].reset_index(drop=True)
                df_final = pd.concat([df_vertical, commit_df], axis=1)
                # Adiciona ao DataFrame principal
                small_files_commits_df = pd.concat([small_files_commits_df, df_final], ignore_index=True)
        # Se o DataFrame final não estiver vazio, salva
        if small_files_commits_df.empty:
            print(f'\033[33mVazio Small: {repo_path}\033[m')
        else:
            small_files_commits_df.to_csv(f"{output_path}small_files/{repo_path}.csv", index=False)
            print(f'\033[32mJuntou Small: {repo_path}\033[m')
    else:
        print(f'\033[33mPulou Small: {repo_path}\033[m')

with ThreadPoolExecutor() as executor:
    executor.map(process_repository, range(len(repositories)))
