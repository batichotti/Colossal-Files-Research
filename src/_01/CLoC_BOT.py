from os import path, listdir, system
import platform


clones_folder = './src/_00/output'
output_folder = './src/_01/output'

#open CLoC
cloc_path = path.abspath("./src/_01/input/cloc.exe")  # CLoC.exe path
system(f'{cloc_path}')

#running CLoC for each cloned repositories
for repo in listdir(clones_folder):
    repo_name = path.join(repo)
    if path.exists(f'{output_folder}/{repo_name}.csv'):
        print(f"\033[31mDestination path (\033[35m{repo_name}.csv\033[31m) already exists and is not an empty directoryn\033[m")
    else:
        system(f'{cloc_path} --by-file-by-lang --csv --out {output_folder}/{repo_name}.csv {clones_folder}/{repo_name}')
        print(f'\n File \033[35m{repo_name}.csv\033[m was created successfully \n')
