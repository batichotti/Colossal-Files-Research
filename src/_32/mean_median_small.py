import pandas as pd
import os

# df['code'] é o nloc

input_path: str = './src/_01/output/'
output_path: str = './src/_32/output/'

# list with repositories that will analyzed
repositories_list_path: str = './src/_00/input/450_Starred_Projects.csv'
small_set: str = './src/_07/output/'

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
    small_set_path: str = f'{small_set}{main_language}/{owner}~{project}.csv'
    if os.path.exists(small_set_path):
        df_repository = pd.read_csv(small_set_path, sep='|')
        small_df = pd.concat([small_df, df_repository])

# Calculate results by language
result = []
for language in small_df['language'].unique():
    small_language_df = small_df[small_df['language'] == language]

    result.append({
        "Language": language,
        "Type": "Small",
        "nLoc Mean": small_language_df['code'].mean(),
        "nLoc Median": small_language_df['code'].median()
    })

# output
os.makedirs(f'{output_path}', exist_ok=True)
pd.DataFrame(result).to_csv(f"{output_path}global_by_language.csv", index=False)
