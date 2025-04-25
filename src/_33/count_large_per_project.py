import pandas as pd
from os import makedirs, path

SEPARATOR = '|'

# SETUP ================================================================================================================

input_path:str = "./src/_33/input/"
output_path = "./src/_33/output/"
makedirs(output_path, exist_ok=True)
makedirs(f"{output_path}files/", exist_ok=True)

repositories_path:str = "./src/_00/input/450_Starred_Projects.csv"
cloc_path:str = "./src/_01/output/"
large_files_path:str = "./src/_03/output/"

repositories:pd.DataFrame = pd.read_csv(repositories_path)
# print(repositories)

cloc_summary_df:pd.DataFrame = pd.DataFrame
large_files_summary_df:pd.DataFrame = pd.DataFrame

# ======================================================================================================================

result = []

for i in range(len(repositories)):
    # getting repository information
    repository, language = repositories.loc[i, ['url', 'main language']]

    repo_path = f"{language}/{repository.split('/')[-2]}~{repository.split('/')[-1]}"
    print(repo_path)

    if path.exists(f"{large_files_path}{repo_path}.csv"):
        project = pd.read_csv(f"{large_files_path}{repo_path}.csv", sep=SEPARATOR)

        result.append(
            {
                "Language": language,
                "Owner": repository.split('/')[-2],
                "Project": repository.split('/')[-1],
                "#Larges": len(project),
            }
        )

pd.DataFrame(result).to_csv(f"{output_path}larges_per_project.csv", index=False)
