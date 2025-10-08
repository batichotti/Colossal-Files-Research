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

# PROJECT ============================================================================================================

project_append: list[pd.DataFrame] = []

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
                if path.exists(f"{output_path}large/{repo_path}/{large_file_name}.csv"):
                    large_file_data = pd.read_csv(f"{output_path}large/{repo_path}/{large_file_name}.csv", sep=SEPARATOR)
                    if not large_file_data.empty:
                        project_append.append(large_file_data)
            project_df = pd.concat(project_append)

            print(f"{repo_owner}~{repo_name}")

            # Seleciona a string que mais se repete na coluna "born_class" e "final_class"
            born_class_major = project_df["born_class"].mode().iloc[0] if not project_df["born_class"].mode().empty else None
            final_class_major = project_df["final_class"].mode().iloc[0] if not project_df["final_class"].mode().empty else None

            # Colunas summary
            summary_columns = {
                "project_name": f"{repo_owner}~{repo_name}",

                "changes_count_mean": project_df["changes_count"].mean(),
                "changes_count_median": project_df["changes_count"].median(),
                "changes_count_min": project_df["changes_count"].min(),
                "changes_count_max": project_df["changes_count"].max(),

                "p99": p99,

                "born_class_major": born_class_major,

                "initial_size_mean": project_df["initial_size"].mean(),
                "initial_size_median": project_df["initial_size"].median(),
                "initial_size_min": project_df["initial_size"].min(),
                "initial_size_max": project_df["initial_size"].max(),

                "final_class_major": final_class_major,

                "final_size_mean": project_df["final_size"].mean(),
                "final_size_median": project_df["final_size"].median(),
                "final_size_min": project_df["final_size"].min(),
                "final_size_max": project_df["final_size"].max(),

                "mean_size": project_df["mean_size"].mean(),
                "median_size": project_df["median_size"].median(),
                "min_size": project_df["min_size"].min(),
                "max_size": project_df["max_size"].max(),

                "first_commit": project_df["first_commit"].min(),
                "last_commit": project_df["last_commit"].max(),

                "mean_delta_time": project_df["mean_delta_time"].mean(),
                "median_delta_time": project_df["median_delta_time"].median(),
                "min_delta_time": project_df["min_delta_time"].min(),
                "max_delta_time": project_df["max_delta_time"].max()
            }

            summary_df = pd.DataFrame([summary_columns])

            # Salva o resultado
            summary_df.to_csv(f"{output_path}large/{repo_path}.csv", index=False, sep=SEPARATOR)

# LANGUAGE ===========================================================================================================
language_append: list[pd.DataFrame] = []
current_language = ""

for i, row in repositories.iterrows():
    repo_url: str = row['url']
    language: str = row['main language']
    repo_name: str = repo_url.split('/')[-1]
    repo_owner: str = repo_url.split('/')[-2]
    repo_path: str = f"{language}/{repo_owner}~{repo_name}"
    print(repo_path)
    makedirs(f"{output_path}large/{language}/", exist_ok=True)
    if path.exists(f"{large_filtered_path}{repo_path}"):
        large_file_list = pd.read_csv(f"{large_files_path}{repo_path}.csv", sep="|")
        if not large_file_list.empty:
            language_files = []
            for large_file_name in large_file_list["path"].apply(lambda x: x.split("/")[-1]).values:
                print(large_file_name)
                file_path = f"{output_path}large/{repo_path}/{large_file_name}.csv"
                if path.exists(file_path):
                    large_file_data = pd.read_csv(file_path, sep=SEPARATOR)
                    if not large_file_data.empty:
                        language_files.append(large_file_data)
            if language_files:
                language_df = pd.concat(language_files)
                print(f"Linguagem: {language}")

                born_class_major = language_df["born_class"].mode().iloc[0] if not language_df["born_class"].mode().empty else None
                final_class_major = language_df["final_class"].mode().iloc[0] if not language_df["final_class"].mode().empty else None

                summary_columns = {
                    "language": language,

                    "changes_count_mean": language_df["changes_count"].mean(),
                    "changes_count_median": language_df["changes_count"].median(),
                    "changes_count_min": language_df["changes_count"].min(),
                    "changes_count_max": language_df["changes_count"].max(),

                    "born_class_major": born_class_major,

                    "initial_size_mean": language_df["initial_size"].mean(),
                    "initial_size_median": language_df["initial_size"].median(),
                    "initial_size_min": language_df["initial_size"].min(),
                    "initial_size_max": language_df["initial_size"].max(),

                    "final_class_major": final_class_major,

                    "final_size_mean": language_df["final_size"].mean(),
                    "final_size_median": language_df["final_size"].median(),
                    "final_size_min": language_df["final_size"].min(),
                    "final_size_max": language_df["final_size"].max(),

                    "mean_size": language_df["mean_size"].mean(),
                    "median_size": language_df["median_size"].median(),
                    "min_size": language_df["min_size"].min(),
                    "max_size": language_df["max_size"].max(),

                    "first_commit": language_df["first_commit"].min(),
                    "last_commit": language_df["last_commit"].max(),

                    "mean_delta_time": language_df["mean_delta_time"].mean(),
                    "median_delta_time": language_df["median_delta_time"].median(),
                    "min_delta_time": language_df["min_delta_time"].min(),
                    "max_delta_time": language_df["max_delta_time"].max()
                }

                summary_df = pd.DataFrame([summary_columns])
                summary_df.to_csv(f"{output_path}large/{language}.csv", index=False, sep=SEPARATOR)
