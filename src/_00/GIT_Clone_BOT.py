import git
import pandas as pd

input_path = './src/_00/input/600_Starred_Projects_N.csv'
input_file = pd.read_csv(input_path)

for repository in input_file['url']:
    local_repo_directory = f"./src/_00/output/{repository.split('/')[-2]}~{repository.split('/')[-1]}"
    print(f"{repository.split('/')[-2]}~{repository.split('/')[-1]}", end="")

    try:
        try:
            repo = git.Repo.clone_from(repository, local_repo_directory, branch='main')
            print(" -> Main branch")
        except git.exc.GitCommandError:
            try:
                repo = git.Repo.clone_from(repository, local_repo_directory, branch='master')
                print(" -> Master branch")
            except git.exc.GitCommandError:
                print(" -> \033[31mNo Main/Master branches found\033[m")

    except git.exc.GitCommandError as e:
        print()
        if "already exists and is not an empty directory" in e.stderr:
            print("\033[31mDestination path already exists and is not an empty directory\033[m")
        else:
            print(f"\033[31mAn error occurred: {e}\033[m")

print('DONE!')

