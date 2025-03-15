import pandas as pd
from os import makedirs, path
from sys import setrecursionlimit

setrecursionlimit(2_000_000)

SEPARATOR = '|'

# Setup =======================================================================================================
input_path: str = "./src/_14/input/"
output_path: str = "./src/_14/output/"

repositories_path: str = "./src/_00/input/450_Starred_Projects.csv"
large_files_list_path: str = "./src/_03/output/"
large_files_commits_path: str = "./src/_10/output/large_files/"
small_files_commits_path: str = "./src/_10/output/small_files/"

# Carrega e ordena repositórios por linguagem
repositories: pd.DataFrame = pd.read_csv(repositories_path).sort_values(by='main language').reset_index(drop=True)

# DataFrames globais
large_files_commits: pd.DataFrame = pd.DataFrame()
small_files_commits: pd.DataFrame = pd.DataFrame()
large_files_list_geral: pd.DataFrame = pd.DataFrame()

# Funções auxiliares =========================================================================================
def together_metric(file_path: str) -> float:
    """Corrige o path para analise em together_change()"""
    # Implementação fictícia para cálculo da métrica
    return "/".join(path.split("/")[6:])

def together_change(repository_commits: pd.DataFrame, large_files_list_df: pd.DataFrame, change_type: str = "large") -> pd.DataFrame:
    """Detecta quais commits de arquivos grandes tiveram mudanças com outros arquivos"""
    #ver com quantos arquivos? quanto eram grandes? quantos eram pequenos

    large_files_list_df['File Path'] = large_files_list_df["path"].apply(lambda x: together_metric(x))
    # agrupar o df "repository_commits" pela coluna ["Hash"] para faze as análises pelas hashs
    # ve se ou na coluna "Local File PATH Old" ou na "Local File PATH New" existe algum arquivo grande em large_files_list_df['File Path']
    # caso exista verificar quantos elementos da mesma hash diferentes do arquivo analisados tem
    # se existir algum outro elemento anota quantos elementos existem
    # bem como verificar quais desses elementos estão na lista de arquivos grandes / verificar o nloc?
    # salvar esses dois valores separados
    # fazer isso para todas as hashs
    # calcular media/avg, mediana, e porcentagem pelo total e total para quantos commits tiveram mudança de um arquivo grande com outro arquivo
    # calcular mesmas medias para quantas dessas mudanças foram com arquivos grandes
    # quantas foram com arquivos não grandes
    # por fim quantas foram com arquivos grandes e tambem com arquivos não grandes

    result: dict = {
        "Type": [change_type],
        "Together Mean": ["Result 1"],
        "Together Median": ["Result 2"],
        "Together TOTAL": ["Result 3"],
        "Together Percentage": ["Result 4"],
        "Together with Large Mean": ["Result 5"],
        "Together with Large Median": ["Result 6"],
        "Together with Large TOTAL": ["Result 7"],
        "Together with Large Percentage": ["Result 8"],
        "Together with Small Mean": ["Result 9"],
        "Together with Small Median": ["Result 10"],
        "Together with Small TOTAL": ["Result 11"],
        "Together with Small Percentage": ["Result 12"]
    }
    return pd.DataFrame(result)

def process_language(lang: str, large: pd.DataFrame, small: pd.DataFrame, large_files_list_df:pd.DataFrame, output_path: str):
    """Processa e salva resultados por linguagem"""
    results:list[pd.DataFrame] = []
    if not large.empty:
        results.append(together_change(large, large_files_list_df, 'large'))
    if not small.empty:
        results.append(together_change(small, large_files_list_df, 'small'))
    
    if results:
        pd.concat(results).to_csv(f"{output_path}/per_languages/{lang}.csv", index=False)

# Processamento principal =====================================================================================
current_language: str = None
current_large: pd.DataFrame = pd.DataFrame()
current_small: pd.DataFrame = pd.DataFrame()
current_large_list: pd.DataFrame = pd.DataFrame()

for i, row in repositories.iterrows():
    repo_url: str = row['url']
    language: str = row['main language']
    repo_name: str = repo_url.split('/')[-1]
    repo_owner: str = repo_url.split('/')[-2]
    repo_path: str = f"{language}/{repo_owner}~{repo_name}"

    print(repo_path)

    # Cria diretórios necessários
    makedirs(f"{output_path}/per_project/{language}", exist_ok=True)
    makedirs(f"{output_path}/per_languages", exist_ok=True)


    # Atualiza acumuladores de linguagem quando muda
    if current_language and (language != current_language):
        process_language(current_language, current_large, current_small, output_path)
        current_large_list = pd.DataFrame()
        current_large = pd.DataFrame()
        current_small = pd.DataFrame()
    
    current_language = language

    large_list_df: pd.DataFrame = pd.read_csv(f"{large_files_list_path}{repo_path}")
    current_large_list = pd.concat([current_large_list, large_list_df])
    large_files_list_geral = pd.concat([large_files_list_geral, large_list_df])
    
    # Processa arquivos grandes
    large_df: pd.DataFrame = pd.DataFrame()
    large_path = f"{large_files_commits_path}{repo_path}.csv"
    if path.exists(large_path):
        large_df: pd.DataFrame = pd.read_csv(large_path, sep=SEPARATOR)
        current_large = pd.concat([current_large, large_df])
        large_files_commits = pd.concat([large_files_commits, large_df])
    
    # Processa arquivos pequenos
    small_path = f"{small_files_commits_path}{repo_path}.csv"
    small_df: pd.DataFrame = pd.DataFrame()
    if path.exists(small_path):
        small_df: pd.DataFrame = pd.read_csv(small_path, sep=SEPARATOR)
        current_small = pd.concat([current_small, small_df])
        small_files_commits = pd.concat([small_files_commits, small_df])
    
    project_results: list[pd.DataFrame] = []
    if not large_df.empty:
        project_results.append(together_change(large_df, large_list_df))
    if not small_df.empty:
        project_results.append(together_change(small_df, large_list_df, 'small'))

    if project_results:
        pd.concat(project_results).to_csv(f"{output_path}/per_project/{repo_path}.csv", index=False)

# Processa última linguagem
if not current_large.empty or not current_small.empty:
    process_language(current_language, current_large, current_small, output_path)

# Resultado global ============================================================================================
final_results: list[pd.DataFrame] = []
if not large_files_commits.empty:
    final_results.append(together_change(large_files_commits, large_files_list_geral))
if not small_files_commits.empty:
    final_results.append(together_change(small_files_commits, large_files_list_geral, 'small'))

if final_results:
    pd.concat(final_results).to_csv(f"{output_path}/global_results.csv", index=False)