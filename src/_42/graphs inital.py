import pandas as pd
from os import makedirs, path
import datetime
import matplotlib.pyplot as plt

SEPARATOR = ';'

# SETUP ================================================================================================================

input_path: str = "./src/_42/input/"
output_path = "./src/_42/output/"

# repositories_path: str = "./src/_00/input/avalonia.csv"
repositories_path:str = "./src/_00/input/450-linux-pytorch.csv"
language_white_list_path: str = "./src/_12/input/white_list.csv"
percentil_path: str = "./src/_02/output/percentis_by_language_filtered.csv"
large_filtered_path: str = "./src/_39/output/large/"
small_filtered_path: str = "./src/_39/output/small/"
large_files_path: str = "./src/_03/output/"
small_files_path: str = "./src/_07/output/"

language_white_list_df = pd.read_csv(language_white_list_path)
percentil_df = pd.read_csv(percentil_path)

repositories: pd.DataFrame = pd.read_csv(repositories_path)

# FILE =================================================================================================================

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

                        # colunas
                        time_col = "date"
                        nloc_col = "n_loc"

                        # preparar dados
                        large_file_data[time_col] = pd.to_datetime(large_file_data[time_col], errors="coerce")
                        df_plot = large_file_data.dropna(subset=[time_col, nloc_col]).sort_values(time_col)
                        # print(df_plot)
                        if df_plot.empty:
                            print("DataFrame is empty after filtering.")
                            continue

                        # plot
                        fig, ax = plt.subplots(figsize=(10, 4))
                        ax.plot(df_plot[time_col], df_plot[nloc_col], marker="o", linewidth=1, label="n_loc")

                        # linha fixa vermelha
                        p99 = large_file_data["border_line"].iloc[0]
                        # print(f"p99: {p99}")
                        if p99 is not None:
                            ax.axhline(p99, color="red", linestyle="--", linewidth=2, label=f"p99 = {p99}")

                        # remover bordas superior e direita
                        ax.spines["top"].set_visible(False)
                        ax.spines["right"].set_visible(False)

                        ax.set_xlabel("time")
                        ax.set_ylabel("n_loc")
                        ax.legend()
                        fig.autofmt_xdate()

                        # salvar figura
                        out_path = f"{output_path}large/{repo_path}/{large_file_name}.png"
                        plt.savefig(out_path, dpi=150, bbox_inches="tight")
                        plt.close(fig)

