import pandas as pd
import os

"""
O salvamento é feito a partir da linguagem principal do projeto/repositório.
Aqui se procuram todos os large files em todos os projetos e os separam em um .csv para cada repositório e em pastas feitas a partir da linguagem principal do repositório.

Exemplo de output
Projeto MPV-PLAYER - Linguagem principal: C
Local de salvamento
output/C(pq é um repositório C)/colocaaq
O que foi salvo:
path|owner|project|language|code
./src/_00/output/C/mpv-player~mpv/player/command.c|mpv-player|mpv|C|6351 -- Um arquivo C
./src/_00/output/C/mpv-player~mpv/player/lua/osc.lua|mpv-player|mpv|Lua|2250 -- Um arquivo Lua

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