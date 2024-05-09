import git

repo_url = 'https://github.com/username/repository.git'
local_repo_directory = '/path/to/local/repository'
git.Repo.clone_from(repo_url, local_repo_directory)

print("Repository cloned successfully!")
