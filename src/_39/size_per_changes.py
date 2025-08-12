import pandas as pd
from os import makedirs, path
import datetime

SEPARATOR = ';'

# SETUP ================================================================================================================

input_path: str = "./src/_39/input/"
output_path = "./src/_39/output/"

repositories_path: str = "./src/_00/input/avalonia.csv"
# repositories_path:str = "./src/_00/input/450-linux-pytorch.csv"
language_white_list_path: str = "./src/_12/input/white_list.csv"
percentil_path: str = "./src/_02/output/percentis_by_language_filtered.csv"
large_filtered_path: str = "./src/_35/output/large/"
small_filtered_path: str = "./src/_35/output/small/"
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

                        # Adiciona coluna border_line
                        large_file_data["border_line"] = p99

                        # Calcula dif e percentual em relacao ao p99
                        large_file_data["border_diff"] = pd.to_numeric(
                            large_file_data["n_loc"], errors="coerce"
                        ) - pd.to_numeric(large_file_data["border_line"], errors="coerce")
                        large_file_data["nloc_borderline_pct"] = pd.to_numeric(
                            large_file_data["n_loc"], errors="coerce"
                        ) / pd.to_numeric(large_file_data["border_line"], errors="coerce")

                        # Garante tipos numéricos
                        large_file_data["n_loc"] = pd.to_numeric(large_file_data["n_loc"], errors="coerce")
                        large_file_data["lines_balance"] = pd.to_numeric(large_file_data["lines_balance"], errors="coerce")

                        # Calcula last_balance_percentage
                        large_file_data["last_balance_percentage"] = (
                            (large_file_data["n_loc"] / (large_file_data["n_loc"] - large_file_data["lines_balance"])) - 1
                        )

                        # Calcula diferença absoluta
                        large_file_data["n_loc_diff"] = large_file_data["n_loc"].diff()

                        # Preenche valores ausentes antes de pct_change
                        large_file_data["n_loc"] = large_file_data["n_loc"].ffill()

                        # Calcula variação percentual sem preenchimento automático
                        large_file_data["n_loc_pct_change"] = large_file_data["n_loc"].pct_change(fill_method=None).fillna(0)

                        # Calcula dias desde última alteração
                        large_file_data["date"] = pd.to_datetime(large_file_data["date"])
                        large_file_data["days_since_last_swap"] = large_file_data["date"].diff().dt.days.fillna(0).astype(int)

                        # Preenche NaNs e ajusta tipos
                        large_file_data["border_diff"] = large_file_data["border_diff"].fillna(0).astype(float)
                        large_file_data["nloc_borderline_pct"] = large_file_data["nloc_borderline_pct"].fillna(0).astype(float)
                        large_file_data["n_loc"] = large_file_data["n_loc"].fillna(0).astype(int)
                        large_file_data["lines_balance"] = large_file_data["lines_balance"].fillna(0).astype(int)
                        large_file_data["last_balance_percentage"] = large_file_data["last_balance_percentage"].fillna(0).astype(float)
                        large_file_data["n_loc_diff"] = large_file_data["n_loc_diff"].fillna(0).astype(int)
                        large_file_data["n_loc_pct_change"] = large_file_data["n_loc_pct_change"].fillna(0).astype(float)
                        large_file_data["days_since_last_swap"] = large_file_data["days_since_last_swap"].fillna(0).astype(int)

                        # Reordena colunas
                        desired_columns = [
                            "file_name",
                            "change_type", "n_loc", "lines_balance", "last_balance_percentage", "size_change",
                            "is_large", "border_line", "border_diff", "nloc_borderline_pct",
                            "n_loc_diff", "n_loc_pct_change", "date", "days_since_last_swap", "swapped_classification",
                            "complexity", "methods", "tokens",
                            "hash", "files_on_commit", "committer_email", "author_email"
                        ]
                        existing_columns = [col for col in desired_columns if col in large_file_data.columns]
                        large_file_data = large_file_data[existing_columns + [col for col in large_file_data.columns if col not in existing_columns]]

                        # Salva
                        large_file_data.to_csv(f"{output_path}large/{repo_path}/{large_file_name}.csv", sep=SEPARATOR, index=False)

    makedirs(f"{output_path}small/{repo_path}/", exist_ok=True)
    if path.exists(f"{small_filtered_path}{repo_path}.csv"):
        small_files_data = pd.read_csv(f"{small_filtered_path}{repo_path}.csv", sep=SEPARATOR)
        if not small_files_data.empty:
            ...
