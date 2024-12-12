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

repositories_path:str = "./src/_00/input/600_Starred_Projects.csv"
cloc_path:str = "./src/_01/output/"
large_files_path:str = "./src/_03/output/"

repositories:pd.DataFrame = pd.read_csv(repositories_path)
# print(repositories)

# ======================================================================================================================

def main()->None:
    for i in range(len(repositories)):
        # getting repository
        repository, language = repositories.loc[i, ['url', 'main language']]
        # print(f"rep: {repository}, lang: {language}")

        repo_path = f"{language}/{repository.split('/')[-2]}~{repository.split('/')[-1]}"
        print(repo_path)

        repository_files_df:pd.DataFrame = pd.read_csv(f"{cloc_path}/{repo_path}.csv", sep=SEPARATOR)
        repository_files_df = repository_files_df.groupby('language')
        repository_files_df = repository_files_df.size()
        # print(repository_files_df)

        large_files_df:pd.DataFrame = pd.read_csv(f"{large_files_path}/{repo_path}.csv", sep=SEPARATOR)
        large_files_df = large_files_df.groupby('language')
        large_files_df = large_files_df.size()
        # print(large_files_df)

        



if (__name__ == "__main__"):
    main()
