from os import path, listdir, system


clones_folder = './src/_00/output'
output_folder = './src/_01/output'
CLoC_path = path.dirname(path.abspath("./src/_01/input/cloc.exe"))  # CLoC.exe path
system(f'cd {CLoC_path}')

for file in listdir(clones_folder):
    file_name = path.join(file)
    if path.exists(f'{output_folder}/{file_name}.csv'):
        print(f"O arquivo {file_name}.csv já existe \n")
    else:
        system(f'cloc --by-file-by-lang --csv --out {output_folder}/{file_name}.csv {clones_folder}/{file_name}')
        print(f'\n Arquivo {file_name}.csv criado \n')
