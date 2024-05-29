from os import path, listdir, system, remove
from platform import system as op_sys
import pandas as pd
SEPARATOR = '|'

def formater(file_path:str, separator:str=','):
    try:
        file = pd.read_csv(file_path, sep=separator, low_memory=False)

        try:
            columns_to_remove = [col for col in file.columns if col.startswith("github.com/AlDanial/cloc")]
            file = file.drop(columns=columns_to_remove) # removing watermark

            sum_index = file.index[file.iloc[:, 0].str.startswith("SUM")]
            file = file.loc[:sum_index[0] - 1] if sum_index.any() else file # removing lines that we are not interested in

            file = file.rename(columns={'filename':'path'})

            file['owner'] = file['path'].apply(lambda x: x.split('\\')[5].split('~')[0])
            file['project'] = file['path'].apply(lambda x: x.split('\\')[5].split('~')[1])
            file['file'] = file['path'].apply(lambda x: x.split('\\')[-1]) # adding columns to the owner, project, and file name

            file = file[['path', 'owner', 'project', 'file', 'language', 'code', 'comment', 'blank']] # rearranging csv

            file.to_csv(file_path, sep=separator, index=False)

        except:
            print('ERROR#???')
    
    except:
        remove(file_path)
        print(f'\033[31mSeparator fucked up, reestract with Windows(\033[35m{file_path}.csv\033[31m)\033[m')

input_path = './src/_00/output'
output_path = './src/_01/output'

if op_sys() == "Windows":
    cloc = path.abspath("./src/_01/input/cloc.exe")  # CLoC.exe path
else:
    cloc = 'cloc' # REMEMBER TO INSTALL CLOC

# running CLoC for each cloned repositories
for language in listdir(input_path):
    if language != f'time~total.csv':
        language_path = path.join(input_path, language)
        for repository in listdir(language_path):
            if path.exists(f'{output_path}/{language}/{repository}.csv'):
                print(f"\033[31mDestination path (\033[35m{repository}.csv\033[31m) already exists and is not an empty directoryn\033[m")
            else:
                if op_sys() == "Windows":
                    system(f'{cloc} --by-file-by-lang --csv-delimiter="{SEPARATOR}" --out {output_path}/{language}/{repository}.csv {input_path}/{language}/{repository}') # runing CLoC
                    formater(f'{output_path}/{language}/{repository}.csv', SEPARATOR)
                else:               
                    system(f'{cloc} --by-file-by-lang --csv --out {output_path}/{language}/{repository}.csv {input_path}/{language}/{repository}') # for Linux
                    formater(f'{output_path}/{language}/{repository}.csv')
                
                if path.exists(f'{output_path}/{language}/{repository}.csv'):
                    print(f'\n File \033[35m{repository}.csv\033[m was created successfully \n')