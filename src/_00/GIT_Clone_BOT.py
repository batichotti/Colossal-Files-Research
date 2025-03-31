import git
import pandas as pd
from datetime import datetime
from os import path
from platform import system as op_sys

# SETUP ================================================================================================================
input_path = './src/_00/input/go.csv'
output_path = './src/_00/output'

input_file = pd.read_csv(input_path)

start = datetime.now()

# Code =================================================================================================================
index = 0
while index < len(input_file):
# Clone ================================================================================================================
    repository, language, branch = input_file.loc[index, ['url', 'main language', 'branch']]

    repo_path = f"{language}/{repository.split('/')[-2]}~{repository.split('/')[-1]}"
    local_repo_directory = f"{output_path}/{repo_path}"
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

    index += 1

#=======================================================================================================================
end = datetime.now()
time = pd.DataFrame({'start': start, 'end': end, 'time_expended': [end - start]})
time.to_csv(f'{output_path}/time~total.csv', index=False)

print('DONE!')
