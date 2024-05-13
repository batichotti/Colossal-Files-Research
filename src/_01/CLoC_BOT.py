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
                if len(linha) == 6 and linha[0] != "project" and linha[0] != file[:-4]:
                    linha.insert(0, "project")
                    linha.insert(1, "author")
                    
                elif len(linha) == 5 and linha[0] != file[:-4]:
                    project_name = file.split('_')[1]
                    author_name = file.split('_')[0]
                    if project_name.endswith('.py'):
                        project_name = project_name[:-4]
                    linha.insert(0, project_name)
                    linha.insert(1, author_name)
                if len(linha) == 7:
                    lista_csv[i].remove(linha[-1])
                if linha[1] == 'SUM' and linha[2] == '':
                    del lista_csv[i:]
        arquivo.close()
        with open(f'{output_folder}/{file}', 'w', newline='') as arquivo:
            writer_ = writer(arquivo)
            writer_.writerows(lista_csv)
            print(f'\nO Arquivo {file} foi formatado com sucesso \n')


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
