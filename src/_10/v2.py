import pandas as pd
from os import makedirs, scandir, path
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

large_files_commits_total: list[str] = []
small_files_commits_total: list[str] = []

def process_repository(i):
    # getting repository information
    repository, language = repositories.loc[i, ['url', 'main language']]

    makedirs(f"{output_path}large_files/{language}", exist_ok=True)
    makedirs(f"{output_path}small_files/{language}", exist_ok=True)
    
    repo_path: str = f"{language}/{repository.split('/')[-2]}~{repository.split('/')[-1]}"
    
    hashs_large, hashs_small = [], []
    
    print(repo_path)
    
    if path.exists(f"{large_files_commits_path}{repo_path}"):
        hashs_large = [folder.name for folder in scandir(f"{large_files_commits_path}{repo_path}") if folder.is_dir()]
        files_large = [
            [file.name[:-4] for file in scandir(f"{large_files_commits_path}{repo_path}/{hash}/files")]
            for hash in hashs_large
        ]
        large_files_commits_df = pd.DataFrame({"hash": hashs_large, "files": files_large})
        large_files_commits_df.to_csv(f"{output_path}large_files/{repo_path}.csv", index=False)
    
    if path.exists(f"{small_files_commits_path}{repo_path}"):
        hashs_small = [folder.name for folder in scandir(f"{small_files_commits_path}{repo_path}") if folder.is_dir()]
        files_small = [
            [file.name[:-4] for file in scandir(f"{small_files_commits_path}{repo_path}/{hash}/files")]
            for hash in hashs_small
        ]
        small_files_commits_df = pd.DataFrame({"hash": hashs_small, "files": files_small})
        small_files_commits_df.to_csv(f"{output_path}small_files/{repo_path}.csv", index=False)
    
    return hashs_large, hashs_small

with ThreadPoolExecutor() as executor:
    results = list(executor.map(process_repository, range(len(repositories))))

for hashs_large, hashs_small in results:
    large_files_commits_total.extend(hashs_large)
    small_files_commits_total.extend(hashs_small)

large_files_commits_total_df = pd.DataFrame(large_files_commits_total, columns=["hash"])
large_files_commits_total_df.to_csv(f"{output_path}large_files.csv", index=False)

small_files_commits_total_df = pd.DataFrame(small_files_commits_total, columns=["hash"])
small_files_commits_total_df.to_csv(f"{output_path}small_files.csv", index=False)

commits_total = list(set(large_files_commits_total + small_files_commits_total))
commits_total_df = pd.DataFrame(commits_total, columns=["hash"])
commits_total_df.to_csv(f"{output_path}commits_total.csv", index=False)

summary = {
    "Total de Commits Large Files": [len(large_files_commits_total)],
    "Total de Commits Small Files": [len(small_files_commits_total)],
    "Total de Commits": [len(commits_total)],
}

summary_df = pd.DataFrame(summary)
summary_df.to_csv(f"{output_path}summary.csv", index=False)

print("\n", summary)