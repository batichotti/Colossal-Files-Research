import git
import pandas as pd
from datetime import datetime
from os import cpu_count
from concurrent.futures import ThreadPoolExecutor

input_path = './src/_00/input/600_Starred_Projects_vLite.csv'
output_path = './src/_00/output'
input_file = pd.read_csv(input_path)

start = datetime.now()
max_workers = cpu_count()

def clone_repository(repository, language, branch):
    local_repo_directory = f"{output_path}/{language}/{repository.split('/')[-2]}~{repository.split('/')[-1]}"
    print(f"{repository.split('/')[-2]}~{repository.split('/')[-1]}", end="")

    try:
        try:
            git.Repo.clone_from(repository, local_repo_directory, branch=branch)
            print(f" -> {branch} branch")
        except git.exc.GitCommandError:
            print(" -> \033[31mNo Default branches found\033[m")
    except git.exc.GitCommandError as e:
        print()
        if "already exists and is not an empty directory" in str(e):
            print("\033[31mDestination path already exists and is not an empty directory\033[m")
        else:
            print(f"\033[31mAn error occurred: {e}\033[m")

# Using ThreadPoolExecutor to parallelize the cloning process
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    for repository, language, branch in zip(input_file['url'], input_file['main language'], input_file['branch']):
        executor.submit(clone_repository, repository, language, branch)

end = datetime.now()
time_df = pd.DataFrame({'start': [start], 'end': [end], 'time_expended': [end - start]})
time_df.to_csv(f'{output_path}/time~total.csv', index=False)

print('DONE!')
