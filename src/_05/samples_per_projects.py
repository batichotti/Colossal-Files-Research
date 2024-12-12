'''
1º - Calcular quantos arquivos de cada linguagem existem no projeto X
2º - Contar quantos large files de cada linguagem existe no projeto
3º - Calcular a proporção
4º - Aplicar a proporção para os small files
5º - Mostrar quantos arquivos pequenos tem disponível no projeto
'''

import pandas as pd

SEPARATOR = '|'

# SETUP ================================================================================================================

input_path:str = "./src/_05/input/"
output_path = "./src/_05/output/"

repositories_path:str = "./src/_00/input/linux.csv"
cloc_path:str = "./src/_01/output/"
large_files_path:str = "./src/_03/output/"
sample_path:str = f"{input_path}files_sample.csv"

repositories:pd.DataFrame = pd.read_csv(repositories_path)
# print(repositories)

sample_df:pd.DataFrame = pd.read_csv(sample_path)
# sample_df = sample_df.drop(columns=["all", "larges", "small", "2%", "3%", "4%", "5%"]).rename(columns={"1%":"small total"})
# print(sample_df)

# ======================================================================================================================

def main()->None:
    for i in range(len(repositories)):
        # getting repository
        repository, language = repositories.loc[i, ['url', 'main language']]
        # print(f"rep: {repository}, lang: {language}")

        repo_path = f"{language}/{repository.split('/')[-2]}~{repository.split('/')[-1]}"
        print(repo_path)

        repository_files_df:pd.DataFrame = pd.read_csv(f"{cloc_path}/{repo_path}.csv", sep=SEPARATOR)
        repository_files_df = repository_files_df.groupby('language').size()
        # print(repository_files_df)

        large_files_df:pd.DataFrame = pd.read_csv(f"{large_files_path}/{repo_path}.csv", sep=SEPARATOR)
        large_files_df = large_files_df.groupby('language').size()
        # print(large_files_df)

        merged_df = pd.concat([repository_files_df, large_files_df], axis=1).dropna()
        merged_df = merged_df.rename(columns={0: 'total'}).rename(columns={1: 'large files'})
        merged_df['small proportion'] = merged_df['large files'] / merged_df['total']
        merged_df['small files'] = merged_df['total'] * merged_df['small proportion']
        print(merged_df)



if (__name__ == "__main__"):
    main()
