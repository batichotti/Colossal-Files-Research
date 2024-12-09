import pandas as pd
import os

input_path: str = './src/_01/output/'
output_path: str = './src/_05/output/'

csv_reference_large_files: str = './src/_02/output/percentis_by_language_filtered.csv'

# Lista de repositórios que serão analisados
repositories_list_path: str = './src/_00/input/600_Starred_Projects.csv'

# Configuração para determinar o nível de agregação
group_by_project_and_language = False  # Definir como True para resultados por projeto e linguagem

# Carregando a lista de repositórios
repositories: pd.DataFrame = pd.read_csv(repositories_list_path, engine='python')

# DataFrame para armazenar os resultados
language_counts = []

for i in range(len(repositories)):
    # Obtendo informações do repositório
    main_language: str = repositories['main language'].loc[i]
    owner: str = repositories['owner'].loc[i]
    project: str = repositories['project'].loc[i]

    # Gerando o caminho para a lista de arquivos do repositório
    cloc_path: str = f'{input_path}{main_language}/{owner}~{project}.csv'

    # Carregando os dados dos arquivos do repositório
    try:
        df_repository = pd.read_csv(cloc_path, sep='|')
        df_meta = pd.read_csv(csv_reference_large_files)
    except FileNotFoundError:
        print(f"Arquivos não encontrados para o repositório: {owner}/{project}")
        continue

    # Mesclando com os percentis de referência
    merged_df = pd.merge(df_repository, df_meta[['language', 'percentil 99']], on='language')

    # Filtrando os arquivos que excedem o percentil 99
    filtered_df = merged_df[merged_df['code'] >= merged_df['percentil 99']]

    # Contando os arquivos por linguagem ou por projeto e linguagem
    if group_by_project_and_language:
        language_count = filtered_df.groupby(['owner', 'project', 'language']).size().reset_index(name='count')
    else:
        language_count = filtered_df['language'].value_counts().reset_index()
        language_count.columns = ['language', 'count']

    # Adicionando os resultados ao acumulador
    language_counts.append(language_count)

# Concatenando os resultados de todos os repositórios
if language_counts:
    final_counts_df = pd.concat(language_counts, ignore_index=True)

    # Gerando a saída
    os.makedirs(output_path, exist_ok=True)

    if group_by_project_and_language:
        output_file = os.path.join(output_path, 'large_files_project_and_language_counts.csv')
    else:
        final_counts_df = final_counts_df.groupby('language', as_index=False).sum()
        output_file = os.path.join(output_path, 'large_files_language_counts.csv')

    final_counts_df.to_csv(output_file, sep='|', index=False)
    print(f"Resultados salvos em {output_file}")
else:
    print("Nenhum dado de arquivos grandes foi processado.")
