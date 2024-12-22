import git
import pandas as pd
from datetime import datetime
from os import path, system, remove, makedirs
from platform import system as op_sys

SEPARATOR = '|'
DIV = '/'

#=======================================================================================================================
def formater(file_path:str, separator:str=','):
    try:
        file = pd.read_csv(file_path, sep=separator, low_memory=False)
        try:
            columns_to_remove = [col for col in file.columns if col.startswith("github.com/AlDanial/cloc")]
            file = file.drop(columns=columns_to_remove) # removing watermark

            sum_index = file.index[file.iloc[:, 0].str.startswith("SUM")]
            file = file.loc[:sum_index[0] - 1] if sum_index.any() else file # removing lines that we are not interested in

            file = file.rename(columns={'filename':'path'})

            file['owner'] = file['path'].apply(lambda x: x.split(DIV)[5].split('~')[0])
            file['project'] = file['path'].apply(lambda x: x.split(DIV)[5].split('~')[1])
            file['file'] = file['path'].apply(lambda x: x.split(DIV)[-1]) # adding columns to the owner, project, and file name

            file = file[['path', 'owner', 'project', 'file', 'language', 'code', 'comment', 'blank']] # rearranging csv

            file.to_csv(file_path, sep=separator, index=False)
        except:
            print('ERROR#???')
            input('primeiro')
            remove(file_path)
    except:
        print(f'\033[31mSeparator error, reprocess with Windows(\033[35m{file_path}.csv\033[31m)\033[m')
        input('segundo')
        remove(file_path)


# SETUP ================================================================================================================
input_clone_path = './src/_00/input/450_Starred_Projects.csv'
output_clone_path = './src/_00/output'

input_file = pd.read_csv(input_clone_path)

input_cloc_path = output_clone_path
output_cloc_path = './src/_01/output'

if op_sys() == "Windows":
    cloc = path.abspath("./src/_01/input/cloc.exe")  # CLoC.exe path
else:
    cloc = 'cloc'  # REMEMBER TO INSTALL CLOC

start = datetime.now()

# Code =================================================================================================================
index = 0
while index < len(input_file):
# Clone ================================================================================================================
    repository, language, branch = input_file.loc[index, ['url', 'main language', 'branch']]

    repo_path = f"{language}/{repository.split('/')[-2]}~{repository.split('/')[-1]}"
    local_repo_directory = f"{output_clone_path}/{repo_path}"
    print(f"{repository.split('/')[-2]}~{repository.split('/')[-1]}", end='')

    if not path.exists(local_repo_directory):
        try:
            try:
                print(f" - Cloning...")
                repo = git.Repo.clone_from(repository, local_repo_directory, branch=branch)
                print(f"Cloned -> {branch} branch")
            except git.exc.GitCommandError:
                print(f" > \033[31mNo Default branches found\033[m - {branch}\n{repository}")
        except git.exc.GitCommandError as e:
            print()
            if "already exists and is not an empty directory" in e.stderr:
                print("\033[31mDestination path already exists and is not an empty directory\033[m")
            else:
                print(f"\033[31mAn error occurred in Clone:\n{e}\033[m")
    print()

# CLoC =================================================================================================================
    try:
        cloc_repo_path = f"{output_cloc_path}/{repo_path}"
        if path.exists(f'{cloc_repo_path}.csv'):
            print(f"\033[31mDestination path (\033[35m{local_repo_directory}.csv\033[31m) already exists and is not an empty directory\n\033[m")
        else:
            system(f'{cloc} --timeout=0 --by-file-by-lang --csv-delimiter="{SEPARATOR}" --out {cloc_repo_path}.csv {local_repo_directory}')  # running CLoC
            formater(f'{cloc_repo_path}.csv', SEPARATOR)
            if path.exists(f'{cloc_repo_path}.csv'):
                print(f'\n File \033[35m{repository.split('/')[-2]}~{repository.split('/')[-1]}.csv\033[m was created successfully \n')
    except Exception as e:
        print(f"\033[31mAn error occurred in CLoC:\n{e}\033[m")

# Verifying ============================================================================================================
    cloc_flag = True

    try:
        cloc_df = pd.read_csv(f'{cloc_repo_path}.csv', sep=SEPARATOR, low_memory=False)

        for file in cloc_df['path']:
            if not path.exists(file):
                cloc_flag = False
                if path.exists(local_repo_directory):
                    remove(local_repo_directory)
                if path.exists(cloc_repo_path):
                    remove(cloc_repo_path)
                break
    except Exception as e:
        print(f"\033[31mAn error occurred in Verifying:\n{e}\033[m")

    if cloc_flag:
        index += 1

#=======================================================================================================================
end = datetime.now()
time = pd.DataFrame({'start': start, 'end': end, 'time_expended': [end - start]})
time.to_csv(f'{output_clone_path}/time~total.csv', index=False)

print('DONE!')
