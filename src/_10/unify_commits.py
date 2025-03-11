import pandas as pd
from os import makedirs, listdir, scandir

SEPARATOR = '|'

# Setup
input_path:str = "./src/_09/input/"
output_path:str = "./src/_09/output/"

repositories_path:str = "./src/_00/input/450_Starred_Projects.csv"
cloc_path:str = "./src/_01/output/"
large_files_commits_path:str = "./src/_04/output/"
small_files_commits_path:str = "./src/_08/output/"

repositories:pd.DataFrame = pd.read_csv(repositories_path)

for i in range(len(repositories)):
    # getting repository information
    repository, language = repositories.loc[i, ['url', 'main language']]

    makedirs(f"{output_path}large_files/{language}", exist_ok=True)
    makedirs(f"{output_path}small_files/{language}", exist_ok=True)
    
    repo_path = f"{language}/{repository.split('/')[-2]}~{repository.split('/')[-1]}/"
    
    hashs = [folder.name for folder in scandir(f"{large_files_commits_path}{repo_path}") if folder.is_dir()]
    
    print(repository)
    print(hashs)
    input()
