'''
1º - Calcular quantos arquivos de cada linguagem existem no projeto
2º - Contar quantos large files de cada linguagem existe no projeto
3º - Calcular a proporção
4º - Aplicar a proporção para os small files
5º - Mostrar quantos arquivos pequenos tem disponível no projeto
'''

import pandas as pd
from math import ceil
from os import makedirs

SEPARATOR = '|'

# FUNCTION =============================================================================================================

def missing(val:int|float)-> float:
    return -1*val if val < 0 else 0.0

# SETUP ================================================================================================================

input_path:str = "./src/_06/input/"
output_path = "./src/_06/output/"

repositories_path:str = "./src/_00/input/450_Starred_Projects.csv"
cloc_path:str = "./src/_01/output/"
large_files_path:str = "./src/_03/output/"
sample_path:str = f"{input_path}files_sample.csv"

repositories:pd.DataFrame = pd.read_csv(repositories_path)
# print(repositories)

# getting small files per language
sample_df:pd.DataFrame = pd.read_csv(sample_path)
small_sample_df:pd.Series = sample_df.set_index('language')['1%']
large_total_df:pd.Series = sample_df.set_index('language')['#large files']

missing_df:pd.Series = pd.Series()

# ======================================================================================================================

def main()->None:
    for i in range(len(repositories)):
        # getting repository information
        repository, language = repositories.loc[i, ['url', 'main language']]

        makedirs(f"{output_path}{language}", exist_ok=True)
        repo_path = f"{language}/{repository.split('/')[-2]}~{repository.split('/')[-1]}"
        print(repo_path)

        # getting a file total per language
        repository_files_df:pd.DataFrame = pd.read_csv(f"{cloc_path}/{repo_path}.csv", sep=SEPARATOR)
        repository_files_df:pd.Series = repository_files_df.groupby('language').size()

        # getting a large file total per language
        large_files_df:pd.DataFrame = pd.read_csv(f"{large_files_path}/{repo_path}.csv", sep=SEPARATOR)
        large_files_df:pd.Series = large_files_df.groupby('language').size()

        # data manipulation
        merged_df = pd.concat([repository_files_df, large_files_df, large_total_df, small_sample_df], axis=1).dropna() # grouping series by languages
        merged_df = merged_df.rename(columns={0: 'total'}).rename(columns={1: 'large files p/ project'}).rename(columns={'#large files': 'large files total'}).rename(columns={'1%': 'small files total'}) # renaming columns
        merged_df['small proportion'] = merged_df['large files p/ project'] / merged_df['large files total'] # large file / total
        merged_df['small files p/ project'] = merged_df['small files total'] * merged_df['small proportion'] # (large file / total) * small total
        merged_df['small files p/ project'] = merged_df['small files p/ project'].apply(ceil) # round up
        merged_df['files available'] = merged_df['total'] - merged_df['large files p/ project'] # total - large files
        merged_df['files missing'] = merged_df['files available'] - merged_df['small files p/ project'] # available - small
        merged_df['files missing'] = merged_df['files missing'].apply(missing) # missing

        merged_df.to_csv(f"{output_path}{repo_path}.csv")

        if i == 0:
            missing_df = pd.concat([merged_df])

        missing_df = pd.concat([missing_df, merged_df])

    missing_df = missing_df.drop(['total', 'large files total', 'small files total', 'small proportion', 'files available'], axis=1)
    missing_df = missing_df.groupby(['language']).agg('sum').rename(columns={'large files p/ project': 'large files'}).rename(columns={'small files p/ project': 'small files'})
    missing_df.to_csv(f"{output_path}missing.csv")

if (__name__ == "__main__"):
    main()
