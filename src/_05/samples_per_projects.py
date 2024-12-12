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

# SETUP ================================================================================================================

input_path:str = "./src/_05/input/"
output_path = "./src/_05/output/"

repositories_path:str = "./src/_00/input/600_Starred_Projects.csv"
cloc_path:str = "./src/_01/output/"
large_files_path:str = "./src/_03/output/"
sample_path:str = f"{input_path}files_sample.csv"

repositories:pd.DataFrame = pd.read_csv(repositories_path)
# print(repositories)

# getting small files per language
sample_df:pd.DataFrame = pd.read_csv(sample_path)
sample_df:pd.Series = sample_df.set_index('language')['1%']

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
        merged_df = pd.concat([repository_files_df, large_files_df, sample_df], axis=1).dropna() # grouping series by languages
        merged_df = merged_df.rename(columns={0: 'total'}).rename(columns={1: 'large files'}).rename(columns={'1%': 'small p/ language'}) # renaming columns
        merged_df['small proportion'] = merged_df['large files'] / merged_df['total'] # large file / total
        merged_df['small files'] = merged_df['small p/ language'] * merged_df['small proportion'] # (large file / total) * small total
        merged_df['small files'] = merged_df['small files'].apply(ceil) # round up
        merged_df['files available'] = merged_df['total'] - merged_df['large files'] # total - large files

        merged_df.to_csv(f"{output_path}{repo_path}.csv")


if (__name__ == "__main__"):
    main()
