import pandas as pd
from os import makedirs, path
import datetime
import matplotlib.pyplot as plt

SEPARATOR = ';'

# SETUP ================================================================================================================

input_path: str = "./src/_43/input/"
output_path = "./src/_43/output/"

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
                            # valor absoluto do percentil 99 para a linguagem
                            try:
                                p99 = float(p99_row["percentil 99"].values[0])
                            except Exception:
                                p99 = None
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
                        large_file_data[nloc_col] = pd.to_numeric(large_file_data[nloc_col], errors="coerce")
                        df_plot = large_file_data.dropna(subset=[time_col, nloc_col]).sort_values(time_col)
                        if df_plot.empty:
                            print("DataFrame is empty after filtering.")
                            continue

                        # precisa do percentil 99 válido para escalar
                        if p99 is None or p99 <= 0:
                            print(f"Skipping {large_file_name}: invalid or missing p99 for language '{language_name if 'language_name' in locals() else language}'.")
                            continue

                        # converter para percentagem em relação ao p99 (100% = p99)
                        df_plot = df_plot.copy()
                        df_plot["nloc_pct"] = df_plot[nloc_col] / p99 * 100

                        # Configurar estilo ABNT
                        plt.rcParams['font.size'] = 12
                        
                        # plot
                        fig, ax = plt.subplots(figsize=(12, 6))
                        
                        # Linha principal com estilo mais acadêmico
                        ax.plot(df_plot[time_col], df_plot["nloc_pct"], 
                               marker="o", markersize=4, linewidth=1.5, 
                               color='#2E86AB', label="NLOC (% do p99)")

                        # linha fixa em 100% (p99)
                        ax.axhline(100, color='#A23B72', linestyle='--', 
                                  linewidth=2, label=f"p99 = 100% ({int(p99)} LOC)")

                        # FIXAR LIMITES DO EIXO Y
                        # De 0 a 100 fixo, depois dinâmico conforme os dados
                        max_value = df_plot["nloc_pct"].max()
                        if max_value > 100:
                            y_max = max_value * 1.1  # 10% de margem acima do máximo
                        else:
                            y_max = 110  # margem pequena se não ultrapassar 100%
                        
                        ax.set_ylim(0, y_max)
                        
                        # Grid sutil (padrão ABNT)
                        ax.grid(True, linestyle=':', alpha=0.3, color='gray')
                        ax.set_axisbelow(True)
                        
                        # Todas as bordas visíveis (padrão ABNT)
                        ax.spines["top"].set_visible(True)
                        ax.spines["right"].set_visible(True)
                        ax.spines["bottom"].set_visible(True)
                        ax.spines["left"].set_visible(True)
                        
                        # Deixar bordas mais finas
                        for spine in ax.spines.values():
                            spine.set_linewidth(0.8)
                            spine.set_color('black')

                        # Título do gráfico com projeto, arquivo e linguagem
                        project_name = f"{repo_owner}/{repo_name}"
                        lang_display = language_name if 'language_name' in locals() and language_name else language
                        ax.set_title(f"Temporal evolution of file size - {project_name} - {large_file_name} ({lang_display})", 
                                    fontsize=12, fontweight='normal', pad=15)
                        
                        # Rótulos dos eixos
                        ax.set_xlabel("Time", fontsize=12, fontweight='normal')
                        ax.set_ylabel("Percentage of p99 (%)", fontsize=12, fontweight='normal')
                        
                        # Legenda com borda
                        ax.legend(loc='best', frameon=True, edgecolor='black', 
                                 fancybox=False, shadow=False, fontsize=11)
                        
                        fig.autofmt_xdate()

                        # salvar figura
                        out_path = f"{output_path}large/{repo_path}/{large_file_name}.png"
                        plt.savefig(out_path, dpi=150, bbox_inches="tight")
                        plt.close(fig)