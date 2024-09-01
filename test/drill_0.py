import pydriller as dr
import pandas as pd
import os
from datetime import datetime

def commit_anal(commit):
    summary = pd.DataFrame({
        'Hash': [commit.hash],
        'Project Name': [commit.project_name],
        'Local Commit PATH': [commit.project_path],
        'Merge Commit': [commit.merge],
        'Message': [commit.msg],
        'Number of Files': [len(commit.modified_files)],
        'Author Name': [commit.author.name],
        'Author Email': [commit.author.email],
        'Author Commit Date': [commit.author_date],
        'Author Commit Timezone': [commit.author_timezone],
        'Committer Name': [commit.committer.name],
        'Committer Email': [commit.committer.email],
        'Committer Commit Date': [commit.committer_date],
        'Committer Timezone': [commit.committer_timezone],
    })
    return summary

def file_anal(file):
    summary = pd.DataFrame({
        'File Name': [file.filename],
        'Change Type': [str(file.change_type).split('.')[-1]],
        'Local File PATH Old': [file.old_path if file.old_path else 'none'],
        'Local File PATH New': [file.new_path],
        'Complexity': [file.complexity if file.complexity else 'null'],
        'Methods': [len(file.methods)],
        'Tokens': [file.token_count if file.token_count else 'null'],
        'Lines Of Code (nloc)': [file.nloc if file.nloc else 'null'],
        'Lines Added': [file.added_lines],
        'Lines Deleted': [file.deleted_lines],
    })
    return summary

# ----------------------------------------------------------------------

# Caminho do repositório
input_path = ['./clone/Java/batichotti~Clube-Do-Filme', 'https://github.com/batichotti/Clube-Do-Filme',]

branch = 'master'
files = ['web/assets/js/cadastroAdm.js',]
# repository = dr.Repository(input_path[1])
repository = dr.Repository(input_path[1], only_in_branch=branch, filepath=files[0])

# Configuração inicial
language = 'Java'
project = 'batichotti~Clube-Do-Filme'
dir_path = f'./mined_commits_pfo/{language}/{project}'
os.makedirs(dir_path, exist_ok=True)

start = datetime.now()

print(project)
counter = 0
for commit in repository.traverse_commits():
    try:
        commit_dir = f'{dir_path}/commit_{counter}/'
        os.makedirs(commit_dir, exist_ok=True)

        # Analisando e salvando informações do commit
        df_commit = commit_anal(commit)
        df_commit.to_csv(f'{commit_dir}/commit.csv')

        # Analisando e salvando informações dos arquivos modificados no commit
        files_dir = f'{commit_dir}/files/'
        os.makedirs(files_dir, exist_ok=True)

        for file in commit.modified_files:
            df_file = file_anal(file)
            df_file.to_csv(f'{files_dir}{file.filename}.csv')

    except Exception as e:
        print(f'Error: {e}')
        # Criação do diretório de erros, caso não exista
        error_dir = './mined_commits/errors/'
        os.makedirs(error_dir, exist_ok=True)

        # Salvando informações do erro no diretório apropriado
        df_commit.to_csv(f'{error_dir}errors_{commit.project_name}.csv', mode='a', header=False)

    counter += 1

# Δt
end = datetime.now()

total_time = pd.DataFrame({
    'Star' : [start],
    'End' : [end],
    'TOTAL' : [end-start],
})
total_time.to_csv(f'{dir_path}/total_time.csv')
print(end-start)
