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
makedirs(f"{output_path}files/", exist_ok=True)

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

    cloc_summary_df.to_csv(f"{output_path}files/all_files.csv")
    cloc_summary_df.groupby('language').size().to_csv(f"{output_path}#all_files.csv")

    large_files_summary_df.to_csv(f"{output_path}files/large_files.csv")
    large_files_summary_df.groupby('language').size().to_csv(f"{output_path}#large_files.csv")
    
    # Filter cloc_summary_df to include only languages present in large_files_summary_df
    filtered_languages = large_files_summary_df['language'].unique()
    filtered_cloc_summary_df = cloc_summary_df[cloc_summary_df['language'].isin(filtered_languages)]

    # Save the filtered summary to a new CSV file
    filtered_cloc_summary_df.to_csv(f"{output_path}files/all_filtered.csv")
    filtered_cloc_summary_df.groupby('language').size().to_csv(f"{output_path}#all_filtered.csv")
    
    # Total + Large files
    summary_df = pd.concat([filtered_cloc_summary_df.groupby('language').size().rename('#total'), large_files_summary_df.groupby('language').size().rename('#large files')], axis=1)
    summary_df["#small files"] = summary_df["#total"] - summary_df['#large files']
    summary_df.to_csv(f"{output_path}#total.csv")
    
if (__name__ == "__main__"):
    main()
    print("DONE!\n")
