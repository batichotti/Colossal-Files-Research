import pandas as pd
from os import makedirs, path
import datetime

SEPARATOR = ';'

# SETUP ================================================================================================================

input_path: str = "./src/_40/input/"
output_path = "./src/_40/output/"

repositories_path: str = "./src/_00/input/avalonia.csv"
# repositories_path:str = "./src/_00/input/450-linux-pytorch.csv"
language_white_list_path: str = "./src/_12/input/white_list.csv"
percentil_path: str = "./src/_02/output/percentis_by_language_filtered.csv"
large_filtered_path: str = "./src/_39/output/large/"
small_filtered_path: str = "./src/_39/output/small/"
large_files_path: str = "./src/_03/output/"
small_files_path: str = "./src/_07/output/"

language_white_list_df = pd.read_csv(language_white_list_path)
percentil_df = pd.read_csv(percentil_path)

repositories: pd.DataFrame = pd.read_csv(repositories_path)

# ======================================================================================================================

for i, row in repositories.iterrows():
    repo_url: str = row['url']
    language: str = row['main language']
    repo_name: str = repo_url.split('/')[-1]
    repo_owner: str = repo_url.split('/')[-2]
    repo_path: str = f"{language}/{repo_owner}~{repo_name}"
    print(repo_path)

    makedirs(f"{output_path}large/{repo_path}/", exist_ok=True)
    if path.exists(f"{large_filtered_path}{repo_path}"):
        large_file_list = pd.read_csv(f"{large_files_path}{repo_path}.csv", sep="|")
        if not large_file_list.empty:
            for large_file_name in large_file_list["path"].apply(lambda x: x.split("/")[-1]).values:
                print(large_file_name)
                if path.exists(f"{large_filtered_path}{repo_path}/{large_file_name}.csv"):
                    large_file_data = pd.read_csv(f"{large_filtered_path}{repo_path}/{large_file_name}.csv", sep=SEPARATOR)

                    extension = large_file_name.split('.')[-1]
                    language_row = language_white_list_df[language_white_list_df["Extension"] == extension]
                    if not language_row.empty:
                        language_name = language_row["Language"].values[0]
                        p99_row = percentil_df[percentil_df["language"] == language_name]
                        if not p99_row.empty:
                            p99 = p99_row["percentil 99"].values[0]
                        else:
                            p99 = None
                    else:
                        p99 = None

                    if not large_file_data.empty:
                        # pega o nome do arquivo
                        large_file_name = large_file_name

                        # pega a quantidade de mudanças
                        changes_count = len(large_file_data)

                        # pega o primeiro valor de is_large
                        born_class = "large" if large_file_data["is_large"].iloc[0] else "small"
                        # pega o último valor de is_large
                        final_class = "large" if large_file_data["is_large"].iloc[-1] else "small"

                        # pega o tamanho inicial do arquivo
                        initial_size = large_file_data["n_loc"].iloc[0]
                        # pega o tamanho final do arquivo
                        final_size = large_file_data["n_loc"].iloc[-1]
                        # calcula a media e mediana de tamanho dos arquivos
                        mean_size = large_file_data["n_loc"].mean()
                        median_size = large_file_data["n_loc"].median()
                        # pega menor tamanho do arquivo
                        min_size = large_file_data["n_loc"].min()
                        # pega maior tamanho do arquivo
                        max_size = large_file_data["n_loc"].max()

                        # pega o primeiro e último commit
                        first_commit = large_file_data["date"].iloc[0]
                        last_commit = large_file_data["date"].iloc[-1]
                        # pega a media e mediana do tempo entre as mudanças
                        mean_delta_time = large_file_data["delta_time"].mean()
                        median_delta_time = large_file_data["delta_time"].median()
                        # pega menor tempo entre as mudanças
                        min_delta_time = large_file_data["delta_time"].min()
                        # pega maior tempo entre as mudanças
                        max_delta_time = large_file_data["delta_time"].max()

                        # salva em um csv
                        summary_data = {
                            "file_name": large_file_name,

                            "changes_count": changes_count,

                            "p99": p99,

                            "born_class": born_class,
                            "initial_size": initial_size,
                            "final_class": final_class,
                            "final_size": final_size,

                            "mean_size": mean_size,
                            "median_size": median_size,
                            "min_size": min_size,
                            "max_size": max_size,

                            "first_commit": first_commit,
                            "last_commit": last_commit,
                            
                            "mean_delta_time": mean_delta_time,
                            "median_delta_time": median_delta_time,
                            "min_delta_time": min_delta_time,
                            "max_delta_time": max_delta_time
                        }

                        summary_df = pd.DataFrame([summary_data])
                        summary_df.to_csv(f"{output_path}large/{repo_path}/{large_file_name}.csv", index=False, sep=SEPARATOR)
