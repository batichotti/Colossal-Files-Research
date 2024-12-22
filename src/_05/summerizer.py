import pandas as pd
from os import makedirs

SEPARATOR = '|'

# FUNCTION =============================================================================================================

def missing(val:int|float)-> float:
    return -1*val if val < 0 else 0.0

# SETUP ================================================================================================================

input_path:str = "./src/_05/input/"
output_path = "./src/_05/output/"
makedirs(output_path, exist_ok=True)

repositories_path:str = "./src/_00/input/450_Starred_Projects.csv"
cloc_path:str = "./src/_01/output/"
large_files_path:str = "./src/_03/output/"

repositories:pd.DataFrame = pd.read_csv(repositories_path)
# print(repositories)

cloc_summary_df:pd.DataFrame = pd.DataFrame
large_files_summary_df:pd.DataFrame = pd.DataFrame

# ======================================================================================================================

def main()->None:
    for i in range(len(repositories)):
        # getting repository information
        repository, language = repositories.loc[i, ['url', 'main language']]

        repo_path = f"{language}/{repository.split('/')[-2]}~{repository.split('/')[-1]}"
        print(repo_path)

        # getting a file total per language
        repository_files_df:pd.DataFrame = pd.read_csv(f"{cloc_path}/{repo_path}.csv", sep=SEPARATOR)

        # getting a large file total per language
        large_files_df:pd.DataFrame = pd.read_csv(f"{large_files_path}/{repo_path}.csv", sep=SEPARATOR)

        if i == 0:
            cloc_summary_df = pd.concat([repository_files_df])
            large_files_summary_df = pd.concat([large_files_df])

        cloc_summary_df = pd.concat([cloc_summary_df, repository_files_df])
        large_files_summary_df = pd.concat([large_files_summary_df, large_files_df])

    cloc_summary_df.to_csv(f"{output_path}all_files.csv")
    cloc_summary_df.groupby('language').size().to_csv(f"{output_path}#all_files.csv")

    large_files_summary_df.to_csv(f"{output_path}large_files.csv")
    large_files_summary_df.groupby('language').size().to_csv(f"{output_path}#large_files.csv")

if (__name__ == "__main__"):
    main()
