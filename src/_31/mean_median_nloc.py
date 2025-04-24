import pandas as pd
import os

# df['code'] é o nloc

input_path: str = './src/_01/output/'
output_path: str = './src/_31/output/'

csv_reference_large_files: str = './src/_02/output/percentis_by_language_filtered.csv'

# list with repositories that will analyzed
repositories_list_path: str = './src/_00/input/450_Starred_Projects.csv'

# loading list of repositories
repositories: pd.DataFrame = pd.read_csv(repositories_list_path, engine='python')

large_df = pd.DataFrame()
small_df = pd.DataFrame()

for i in range(len(repositories)):
    # getting repository information
    main_language: str = repositories['main language'].loc[i]
    owner: str = repositories['owner'].loc[i]
    project: str = repositories['project'].loc[i]
    print(f"{main_language}/{owner}~{project}")

    # generating the path to the repository's files list
    repository_path: str = f'{output_path}{main_language}/{owner}~{project}.csv'
    cloc_path: str = f'{input_path}{main_language}/{owner}~{project}.csv'

    df_repository = pd.read_csv(cloc_path, sep='|')
    df_meta = pd.read_csv(csv_reference_large_files)

    merged_df = pd.merge(df_repository, df_meta[['language', 'percentil 99']], on='language')

    filtered_df = merged_df[merged_df['code'] >= merged_df['percentil 99']]
    filtered_small_df = merged_df[merged_df['code'] < merged_df['percentil 99']]

    large_df = pd.concat([large_df, filtered_df[['path', 'owner', 'project', 'language', 'code']]])
    small_df = pd.concat([small_df, filtered_small_df[['path', 'owner', 'project', 'language', 'code']]])

# Calculate results by language
result = []
for language in large_df['language'].unique():
    large_language_df = large_df[large_df['language'] == language]
    small_language_df = small_df[small_df['language'] == language]

    result.append({
        "Language": language,
        "Type": "Large",
        "nLoc Mean": large_language_df['code'].mean(),
        "nLoc Median": large_language_df['code'].median()
    })

    result.append({
        "Language": language,
        "Type": "Small",
        "nLoc Mean": small_language_df['code'].mean(),
        "nLoc Median": small_language_df['code'].median()
    })

# output
os.makedirs(f'{output_path}', exist_ok=True)
pd.DataFrame(result).to_csv(f"{output_path}global_by_language.csv", index=False)
