import pandas as pd
from os import makedirs, listdir, scandir, path

SEPARATOR = '|'

# Setup
input_path:str = "./src/_10/input/"
output_path:str = "./src/_10/output/"

repositories_path:str = "./src/_00/input/450_Starred_Projects.csv"
large_files_commits_path:str = "./src/_04/output/"
small_files_commits_path:str = "./src/_08/output/"

repositories:pd.DataFrame = pd.read_csv(repositories_path)

large_files_commits_total: list[str] = []
small_files_commits_total: list[str] = []

for i in range(len(repositories)):
    # getting repository information
    repository, language = repositories.loc[i, ['url', 'main language']]

    makedirs(f"{output_path}large_files/{language}", exist_ok=True)
    makedirs(f"{output_path}small_files/{language}", exist_ok=True)
    
    repo_path: str = f"{language}/{repository.split('/')[-2]}~{repository.split('/')[-1]}"
    
    hashs: list = []
    
    print(repo_path)
    
    if (path.exists(f"{large_files_commits_path}{repo_path}")):
        hashs: list[str] = [folder.name for folder in scandir(f"{large_files_commits_path}{repo_path}") if folder.is_dir()]

        large_files_commits_df: pd.DataFrame = pd.DataFrame(hashs, columns=["hash"])
        large_files_commits_df.to_csv(f"{output_path}large_files/{repo_path}.csv", index=False)
        
        large_files_commits_total.extend(hashs)
    
        # print(hashs)

    if (path.exists(f"{small_files_commits_path}{repo_path}")):
        hashs: list[str] = [folder.name for folder in scandir(f"{small_files_commits_path}{repo_path}") if folder.is_dir()]
    
        small_files_commits_df: pd.DataFrame = pd.DataFrame(hashs, columns=["hash"])
        small_files_commits_df.to_csv(f"{output_path}small_files/{repo_path}.csv", index=False)
    
        small_files_commits_total.extend(hashs)
        
        # print(hashs)

    # input()

large_files_commits_total_df: pd.DataFrame = pd.DataFrame(large_files_commits_total, columns=["hash"])
large_files_commits_total_df.to_csv(f"{output_path}large_files.csv", index=False)

small_files_commits_total_df: pd.DataFrame = pd.DataFrame(small_files_commits_total, columns=["hash"])
small_files_commits_total_df.to_csv(f"{output_path}small_files.csv", index=False)

commits_total = large_files_commits_total + small_files_commits_total
commits_total = list(set(commits_total))
commits_total_df: pd.DataFrame = pd.DataFrame(commits_total, columns=["hash"])
commits_total_df.to_csv(f"{output_path}small_files.csv", index=False)

summary = {
    "Total de Commits Large Files" : [len(large_files_commits_total)],
    "Total de Commits Small Files" : [len(small_files_commits_total)],
    "Total de Commits" : [len(commits_total)],
}

summary_df: pd.DataFrame = pd.DataFrame(summary)
summary_df.to_csv(f"{output_path}summary.csv", index=False)

print("\n", summary)