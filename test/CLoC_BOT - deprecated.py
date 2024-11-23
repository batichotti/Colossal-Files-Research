from os import path, listdir, system, remove, cpu_count
from platform import system as op_sys
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

SEPARATOR = '|'

def formater(file_path: str, separator: str = ','):
    try:
        file = pd.read_csv(file_path, sep=separator, low_memory=False)
        try:
            columns_to_remove = [col for col in file.columns if col.startswith("github.com/AlDanial/cloc")]
            file = file.drop(columns=columns_to_remove)  # removing watermark

            sum_index = file.index[file.iloc[:, 0].str.startswith("SUM")]
            file = file.loc[:sum_index[0] - 1] if sum_index.any() else file  # removing lines that we are not interested in

            file = file.rename(columns={'filename': 'path'})

            file['owner'] = file['path'].apply(lambda x: x.split('\\')[5].split('~')[0])
            file['project'] = file['path'].apply(lambda x: x.split('\\')[5].split('~')[1])
            file['file'] = file['path'].apply(lambda x: x.split('\\')[-1])  # adding columns to the owner, project, and file name

            file = file[['path', 'owner', 'project', 'file', 'language', 'code', 'comment', 'blank']]  # rearranging csv

            file.to_csv(file_path, sep=separator, index=False)
        except Exception as e:
            print(f'Error during formatting: {e}')
    except Exception as e:
        remove(file_path)
        print(f'\033[31mSeparator error, re-extract with Windows(\033[35m{file_path}.csv\033[31m)\033[m Error: {e}')

def process_repository(cloc_path, input_path, output_path, language, repository):
    repo_input_path = f'{input_path}/{language}/{repository}'
    repo_output_path = f'{output_path}/{language}/{repository}.csv'

    if path.exists(repo_output_path):
        print(f"\033[31mDestination path (\033[35m{repository}.csv\033[31m) already exists and is not an empty directory\033[m")
    else:
        if op_sys() == "Windows":
            system(f'{cloc_path} --by-file-by-lang --csv-delimiter="{SEPARATOR}" --out {repo_output_path} {repo_input_path}')  # run CLoC
        else:
            system(f'{cloc_path} --by-file-by-lang --csv --out={repo_output_path} {repo_input_path}')  # for Linux

        formater(repo_output_path, SEPARATOR)
        
        if path.exists(repo_output_path):
            print(f'\n File \033[35m{repository}.csv\033[m was created successfully \n')

input_path = './src/_00/output'
output_path = './src/_01/output'
cloc_path = path.abspath("./src/_01/input/cloc.exe") if op_sys() == "Windows" else 'cloc'
max_workers = cpu_count()

# Using ThreadPoolExecutor to parallelize the process
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    for language in listdir(input_path):
        if language != 'time~total.csv':
            language_path = path.join(input_path, language)
            for repository in listdir(language_path):
                executor.submit(process_repository, cloc_path, input_path, output_path, language, repository)
