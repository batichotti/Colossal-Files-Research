import pandas as pd
import os

"""
The saving is done based on the main language of the project/repository.
Here, all large files in all projects are searched and separated into a .csv for each repository and in folders made from the main language of the repository.

Example of output
Project MPV-PLAYER - Main language: C
Save location
output/C (because it is a C repository)/put here
What was saved:
path|owner|project|language|code
./src/_00/output/C/mpv-player~mpv/player/command.c|mpv-player|mpv|C|6351 -- A C file
./src/_00/output/C/mpv-player~mpv/player/lua/osc.lua|mpv-player|mpv|Lua|2250 -- A Lua file
"""

# df['code'] é o nloc

input_path: str = './src/_01/output/'
output_path: str = './src/_03/output/'

csv_reference_large_files: str = './src/_02/output/percentis_by_language_filtered.csv'

# list with repositories that will analyzed
repositories_list_path: str = './src/_00/input/600_Starred_Projects.csv'

# loading list of repositories
repositories: pd.DataFrame = pd.read_csv(repositories_list_path, engine='python')


for i in range(len(repositories)):
    # getting repository information
    main_language:str = repositories['main language'].loc[i]
    owner:str = repositories['owner'].loc[i]
    project:str = repositories['project'].loc[i]

    # generating the path to the repository's files list
    repository_path: str = f'{output_path}{main_language}/{owner}~{project}.csv'
    cloc_path: str = f'{input_path}{main_language}/{owner}~{project}.csv'

    df_repository = pd.read_csv(cloc_path, sep='|')
    df_meta = pd.read_csv(csv_reference_large_files)

    merged_df = pd.merge(df_repository, df_meta[['language', 'percentil 99']], on='language')

    filtered_df = merged_df[merged_df['code'] >= merged_df['percentil 99']]

    final_df = filtered_df[['path', 'owner', 'project', 'language', 'code']]

    # output
    os.makedirs(f'{output_path}{main_language}/', exist_ok=True)
    final_df.to_csv(repository_path, sep='|', index=False)
