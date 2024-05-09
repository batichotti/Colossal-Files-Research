import git
import os
from input import Repos as repositories


for repository in repositories.list:
    local_repo_directory = f'./src/00/output/{repository.split('/')[-2]}_{repository.split('/')[-1]}'
    print(f'{repository.split('/')[-2]}/{repository.split('/')[-1]}')

    try:
        repo = git.Repo.clone_from(repository, local_repo_directory)
        try:
            repo.git.checkout('main')
            print(" Main branch")
        except git.exc.GitCommandError:
            try:
                repo.git.checkout('master')
                print(" Master branch")
            except git.exc.GitCommandError:
                print("No Main/Master branch found")
    except git.exc.GitCommandError as e:
        if 'already exists and is not an empty directory' in e.stderr:
            print("Destination path already exists and is not an empty directory.")
        else:
            print("An error occurred:", e)

print('DONE!')
