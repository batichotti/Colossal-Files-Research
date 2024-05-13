from os import path, listdir, system
from platform import system as op_sys
from csv import reader, writer


def formater(output_folder='./output'):
    """ Format the .csv file with a project variable, and delete the SUM data """
    for file in listdir(output_folder):
        with open(f'{output_folder}/{file}', 'r') as arquivo:
            arquivo_csv = reader(arquivo, delimiter=',')
            lista_csv = list(arquivo_csv)
            for i, linha in enumerate(lista_csv):
                project_name = file.split('_')[1][:-4]
                author_name = file.split('_')[0]
                if len(linha) == 6 and linha[0] != "project" and linha[0] != project_name:
                    linha.insert(0, "project")
                    linha.insert(1, "author")
                    linha[3] = "path"
                    linha.insert(4, "filename")
                    del linha[-1]
                elif len(linha) == 5 and linha[0] != file[:-4]:
                    linha.insert(0, project_name)
                    linha.insert(1, author_name)
                    linha.insert(4, linha[3].split('\\')[-1])
                if linha[2] == 'SUM' and linha[3] == '':
                    del lista_csv[i:]
        arquivo.close()
        with open(f'{output_folder}/{file}', 'w', newline='') as arquivo:
            writer_ = writer(arquivo)
            writer_.writerows(lista_csv)
            print(f'\nO Arquivo \033[35m{file}\033[m foi formatado com sucesso \n')


clones_folder = './src/_00/output'
output_folder = './src/_01/output'
if op_sys() == "Windows":
    cloc = path.abspath("./src/_01/input/cloc.exe")  # CLoC.exe path
else:
    cloc = 'cloc' # REMEMBER TO INSTALL CLOC

# running CLoC for each cloned repositories
for repo in listdir(clones_folder):
    repo_name = path.join(repo)
    if path.exists(f'{output_folder}/{repo_name}.csv'):
        print(f"\033[31mDestination path (\033[35m{repo_name}.csv\033[31m) already exists and is not an empty directoryn\033[m")
    else:
        system(f'{cloc} --by-file-by-lang --csv --out {output_folder}/{repo_name}.csv {clones_folder}/{repo_name}')
        print(f'\n File \033[35m{repo_name}.csv\033[m was created successfully \n')

formater(output_folder)
