import git
import pandas as pd
from datetime import datetime
from os import cpu_count, path
from concurrent.futures import ThreadPoolExecutor

input_path = './src/_00/input/600_Starred_Projects_vLite.csv'
output_path = './src/_00/output'
input_file = pd.read_csv(input_path)

start = datetime.now()
max_workers = cpu_count()

def clone_repository(repository, language, branch):
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
                input()
        except git.exc.GitCommandError as e:
            print()
            if "already exists and is not an empty directory" in e.stderr:
                print("\033[31mDestination path already exists and is not an empty directory\033[m")
            else:
                print(f"\033[31mAn error occurred in Clone:\n{e}\033[m")
    print()

with ThreadPoolExecutor(max_workers=max_workers) as executor:
    for repository, language, branch in zip(input_file['url'], input_file['main language'], input_file['branch']):
        executor.submit(clone_repository, repository, language, branch)

end = datetime.now()
time_df = pd.DataFrame({'start': [start], 'end': [end], 'time_expended': [end - start]})
time_df.to_csv(f'{output_path}/time~total.csv', index=False)

print('DONE!')
