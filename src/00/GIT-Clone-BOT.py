import git
import input.Repos as repositories

for repository in repositories.list:
    local_repo_directory = f"./src/00/output/{repository.split('/')[-2]}_{repository.split('/')[-1]}"
    print(f"{repository.split('/')[-2]}/{repository.split('/')[-1]}", end="")

    try:
        try:
            repo = git.Repo.clone_from(repository, local_repo_directory, branch='main')
            print("- Main branch")
        except git.exc.GitCommandError:
            try:
                repo = git.Repo.clone_from(repository, local_repo_directory, branch='master')
                print("- Master branch")
            except git.exc.GitCommandError:
                print("- No Main/Master branches found")

    except git.exc.GitCommandError as e:
        print()
        if "already exists and is not an empty directory" in e.stderr:
            print("Destination path already exists and is not an empty directory.")
        else:
            print("An error occurred:", e)

print('DONE!')
