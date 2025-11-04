import pandas as pd
import matplotlib.pyplot as plt
from os import makedirs, path, listdir

SEPARATOR = ';'

input_path: str = "./src/_41/input/"
output_path = "./src/_41/output/"

large_files_path: str = "./src/_40/output/large"
small_files_path: str = "./src/_40/output/small"

percentil_path: str = "./src/_02/output/percentis_by_language_filtered.csv"
percentil_df = pd.read_csv(percentil_path) # ['language'] e ['percentil 99']

white_list_path: str = "./src/_12/input/white_list.csv"
white_list_df = pd.read_csv(white_list_path)

def get_mean_and_median_by_project(type: str = "large") -> pd.DataFrame:
    data = {
        "Project": [],
        "Language": [],  # main language of the project
        "Initial_Mean": [],
        "Final_Mean": [],
        "Initial_Median": [],
        "Final_Median": []
    }
    files_path = large_files_path if (type.lower() == "large") else small_files_path
    for folder in listdir(files_path):
        if not (folder.endswith(".csv")):
            for item in listdir(path.join(files_path, folder)):
                if (item.endswith(".csv")):
                    df = pd.read_csv(path.join(files_path, folder, item), sep=SEPARATOR)
                    data["Project"].append(df['project_name'].values[0])
                    data["Language"].append(folder)
                    data["Initial_Mean"].append(df['initial_size_mean'].values[0])
                    data["Final_Mean"].append(df['final_size_mean'].values[0])
                    data["Initial_Median"].append(df['initial_size_median'].values[0])
                    data["Final_Median"].append(df['final_size_median'].values[0])

    return pd.DataFrame(data)

def get_mean_and_median_by_project(type: str = "large") -> pd.DataFrame:
    data = {
        "Project": [],
        "Language": [],  # main language of the project
        "Initial_Mean": [],
        "Final_Mean": [],
        "Initial_Median": [],
        "Final_Median": []
    }
    files_path = large_files_path if (type.lower() == "large") else small_files_path
    for folder in listdir(files_path):
        if not (folder.endswith(".csv")):
            for item in listdir(path.join(files_path, folder)):
                if (item.endswith(".csv")):
                    df = pd.read_csv(path.join(files_path, folder, item), sep=SEPARATOR)
                    data["Project"].append(df['project_name'].values[0])
                    data["Language"].append(folder)
                    data["Initial_Mean"].append(df['initial_size_mean'].values[0])
                    data["Final_Mean"].append(df['final_size_mean'].values[0])
                    data["Initial_Median"].append(df['initial_size_median'].values[0])
                    data["Final_Median"].append(df['final_size_median'].values[0])

    return pd.DataFrame(data)

def get_mean_and_median_by_language(type: str = "large") -> pd.DataFrame:
    data = {
        "Project": [],
        "Language": [],
        "Initial_Size": [],
        "Final_Size": []
    }
    files_path = large_files_path if (type.lower() == "large") else small_files_path
    extensions = white_list_df['Extension'].unique()  # Filtrar extensões permitidas
    for folder in listdir(files_path):
        if not (folder.endswith(".csv")):
            for item in listdir(path.join(files_path, folder)):
                if not (item.endswith(".csv")):
                    for file in listdir(path.join(files_path, folder, item)):
                        if any(file[:-4].endswith(ext) for ext in extensions) and (file.endswith(".csv")):  # Filtrar por extensão
                            df = pd.read_csv(path.join(files_path, folder, item, file), sep=SEPARATOR)
                            if file[:-4].split('.')[-1] in ['podspec', 'gemspec', 'pac']:
                                continue
                            data["Project"].append(item)
                            language = white_list_df[white_list_df['Extension'] == file[:-4].split('.')[-1]]['Language'].iloc[0]
                            data["Language"].append(language)
                            data["Initial_Size"].append(df['initial_size'].values[0])
                            data["Final_Size"].append(df['final_size'].values[0])

    return pd.DataFrame(data)

def generate_graphs_initial_x_final(df: pd.DataFrame, subfolder: str = "", mode: str = "project") -> None:
        folder_path = path.join(output_path, subfolder)
        makedirs(folder_path, exist_ok=True)
        df = df.merge(percentil_df, left_on='Language', right_on='language', how='left')
        languages = df['Language'].unique()

        if mode == "project":
            for lang in languages:
                lang_df = df[df['Language'] == lang].reset_index(drop=True)
                if lang_df.empty:
                    continue
                # Para cada linguagem, plota todos os projetos dessa linguagem juntos
                projects = lang_df['Project']
                x = range(len(projects))
                perc99_value = lang_df['percentil 99'].iloc[0]

                # Gráfico para Médias por Linguagem (todos projetos dessa linguagem)
                plt.figure(figsize=(14, 6))
                width = 0.35
                plt.bar([i - width/2 for i in x], lang_df['Initial_Mean'], width, label='Initial Mean', color='black')
                plt.bar([i + width/2 for i in x], lang_df['Final_Mean'], width, label='Final Mean', color='gray')
                plt.axhline(y=perc99_value, color='red', linestyle='-', linewidth=1, label='Percentil 99')
                plt.xlabel('Projects')
                plt.ylabel('Size (Mean)')
                plt.title(f'Initial vs Final Size - Language: {lang} (Mean)')
                plt.xticks(x, projects, rotation=90, fontsize=8)
                plt.legend()
                plt.tight_layout()
                plt.savefig(path.join(folder_path, f'mean_language_{lang}.png'), dpi=150)
                plt.close()

                # Gráfico para Medianas por Linguagem (todos projetos dessa linguagem)
                plt.figure(figsize=(14, 6))
                plt.bar([i - width/2 for i in x], lang_df['Initial_Median'], width, label='Initial Median', color='black')
                plt.bar([i + width/2 for i in x], lang_df['Final_Median'], width, label='Final Median', color='gray')
                plt.axhline(y=perc99_value, color='red', linestyle='-', linewidth=1, label='Percentil 99')
                plt.xlabel('Projects')
                plt.ylabel('Size (Median)')
                plt.title(f'Initial vs Final Size - Language: {lang} (Median)')
                plt.xticks(x, projects, rotation=90, fontsize=8)
                plt.legend()
                plt.tight_layout()
                plt.savefig(path.join(folder_path, f'median_language_{lang}.png'), dpi=150)
                plt.close()

        elif mode == "language":
        #     filtered_languages = white_list_df['Language'].unique()
        #     for lang in filtered_languages:
        #         lang_df = df[df['Language'] == lang].reset_index(drop=True)
        #         if lang_df.empty:
        #             continue

        #         projects = lang_df['Project']
        #         x = range(len(projects))
        #         perc99_value = lang_df['percentil 99'].iloc[0]

        #         # Gráfico para Médias
        #         plt.figure(figsize=(14, 6))
        #         width = 0.35
        #         plt.bar([i - width/2 for i in x], lang_df['Initial_Size'], width, label='Initial Size', color='black')
        #         plt.bar([i + width/2 for i in x], lang_df['Final_Size'], width, label='Final Size', color='gray')
        #         plt.axhline(y=perc99_value, color='red', linestyle='-', linewidth=1, label='Percentil 99')
        #         plt.xlabel('Projects')
        #         plt.ylabel('Size')
        #         plt.title(f'Initial vs Final Size - Language: {lang} (Mean)')
        #         plt.xticks(x, projects, rotation=90, fontsize=8)
        #         plt.legend()
        #         plt.tight_layout()
        #         plt.savefig(path.join(folder_path, f'language_{lang}.png'), dpi=150)
        #         plt.close()
            filtered_languages = white_list_df['Language'].unique()
            mean_initial_sizes = []
            mean_final_sizes = []
            median_initial_sizes = []
            median_final_sizes = []

            for lang in filtered_languages:
                lang_df = df[df['Language'] == lang].reset_index(drop=True)
                if lang_df.empty:
                    continue

                mean_initial_sizes.append(lang_df['Initial_Size'].mean())
                mean_final_sizes.append(lang_df['Final_Size'].mean())
                median_initial_sizes.append(lang_df['Initial_Size'].median())
                median_final_sizes.append(lang_df['Final_Size'].median())

            x = range(len(filtered_languages))
            # Gráfico para Médias
            plt.figure(figsize=(14, 6))
            width = 0.35
            plt.bar([i - width/2 for i in x], mean_initial_sizes, width, label='Mean Initial Size', color='black')
            plt.bar([i + width/2 for i in x], mean_final_sizes, width, label='Mean Final Size', color='gray')
            for i, lang in enumerate(filtered_languages):
                perc99_value = percentil_df[percentil_df['language'] == lang]['percentil 99'].iloc[0]
                plt.bar(i, perc99_value, width, color='red', alpha=0.5)
            plt.xlabel('Languages')
            plt.ylabel('Size')
            plt.title('Mean Initial vs Final Size by Language')
            plt.xticks(x, filtered_languages, rotation=90, fontsize=8)
            plt.legend(['Mean Initial Size', 'Mean Final Size', 'Percentil 99'])
            plt.tight_layout()
            plt.savefig(path.join(folder_path, 'mean_sizes_by_language.png'), dpi=150)
            plt.close()

            # Gráfico para Medianas
            plt.figure(figsize=(14, 6))
            plt.bar([i - width/2 for i in x], median_initial_sizes, width, label='Median Initial Size', color='black')
            plt.bar([i + width/2 for i in x], median_final_sizes, width, label='Median Final Size', color='gray')
            for i, lang in enumerate(filtered_languages):
                perc99_value = percentil_df[percentil_df['language'] == lang]['percentil 99'].iloc[0]
                plt.bar(i, perc99_value, width, color='red', alpha=0.5)
            plt.xlabel('Languages')
            plt.ylabel('Size')
            plt.title('Median Initial vs Final Size by Language')
            plt.xticks(x, filtered_languages, rotation=90, fontsize=8)
            plt.legend(['Median Initial Size', 'Median Final Size', 'Percentil 99'])
            plt.tight_layout()
            plt.savefig(path.join(folder_path, 'median_sizes_by_language.png'), dpi=150)
            plt.close()

        elif mode == "all":
            # plt.figure(figsize=(14, 6))
            # plt.scatter(df['Initial_Size'], df['Final_Size'], label='Mean', alpha=0.7, color='gray')
            # plt.xlabel('Initial Size')
            # plt.ylabel('Final Size')
            # plt.title('Scatter Plot - All Items')
            # plt.legend()
            # plt.tight_layout()
            # plt.savefig(path.join(folder_path, 'scatter_all.png'), dpi=150)
            # plt.close()
            # # 1. Hexbin Plot com linha diagonal
            # plt.figure(figsize=(14, 10))

            # # Criar hexbin
            # hexbin = plt.hexbin(df['Initial_Size'], df['Final_Size'], gridsize=30, cmap='YlOrRd', mincnt=1)

            # # Adicionar linha diagonal (y=x)
            # max_val = max(df['Initial_Size'].max(), df['Final_Size'].max())
            # min_val = min(df['Initial_Size'].min(), df['Final_Size'].min())
            # plt.plot([min_val, max_val], [min_val, max_val], 'b--', linewidth=2, label='No Change (y=x)')

            # plt.colorbar(hexbin, label='Count')
            # plt.xlabel('Initial Size (LOC)', fontsize=12)
            # plt.ylabel('Final Size (LOC)', fontsize=12)
            # plt.title('Initial vs Final Size Distribution\n(Above line = Growth, Below line = Reduction)', fontsize=14)
            # plt.legend(fontsize=10)
            # plt.tight_layout()
            # plt.savefig(path.join(folder_path, 'hexbin_all.png'), dpi=150)
            # plt.close()

            # # 2. Histograma de Diferenças
            # plt.figure(figsize=(14, 6))
            # differences = df['Final_Size'] - df['Initial_Size']

            # plt.hist(differences, bins=50, color='steelblue', edgecolor='black', alpha=0.7)
            # plt.axvline(x=0, color='red', linestyle='--', linewidth=2, label='No Change')
            # plt.axvline(x=differences.mean(), color='green', linestyle='-', linewidth=2, label=f'Mean Difference: {differences.mean():.2f}')
            # plt.axvline(x=differences.median(), color='orange', linestyle='-', linewidth=2, label=f'Median Difference: {differences.median():.2f}')

            # plt.xlabel('Size Difference (Final - Initial)', fontsize=12)
            # plt.ylabel('Frequency', fontsize=12)
            # plt.title('Distribution of Size Changes\n(Positive = Growth, Negative = Reduction)', fontsize=14)
            # plt.legend(fontsize=10)
            # plt.tight_layout()
            # plt.savefig(path.join(folder_path, 'histogram_differences.png'), dpi=150)
            # plt.close()

            # # 3. Box Plot Comparativo
            # plt.figure(figsize=(10, 8))

            # data_to_plot = [df['Initial_Size'], df['Final_Size']]
            # bp = plt.boxplot(data_to_plot, labels=['Initial Size', 'Final Size'],
            #                 patch_artist=True, showmeans=True)

            # # Colorir as caixas
            # colors = ['lightblue', 'lightcoral']
            # for patch, color in zip(bp['boxes'], colors):
            #     patch.set_facecolor(color)

            # plt.ylabel('Size (LOC)', fontsize=12)
            # plt.title('Initial vs Final Size Distribution\n(Box Plot Comparison)', fontsize=14)
            # plt.grid(axis='y', alpha=0.3)
            # plt.tight_layout()
            # plt.savefig(path.join(folder_path, 'boxplot_comparison.png'), dpi=150)
            # plt.close()

            # # 4. Estatísticas resumidas em arquivo de texto
            # stats_path = path.join(folder_path, 'statistics_summary.txt')
            # with open(stats_path, 'w') as f:
            #     f.write("=== SIZE CHANGE STATISTICS ===\n\n")
            #     f.write(f"Total items analyzed: {len(df)}\n\n")

            #     f.write("Initial Size:\n")
            #     f.write(f"  Mean: {df['Initial_Size'].mean():.2f}\n")
            #     f.write(f"  Median: {df['Initial_Size'].median():.2f}\n")
            #     f.write(f"  Std Dev: {df['Initial_Size'].std():.2f}\n\n")

            #     f.write("Final Size:\n")
            #     f.write(f"  Mean: {df['Final_Size'].mean():.2f}\n")
            #     f.write(f"  Median: {df['Final_Size'].median():.2f}\n")
            #     f.write(f"  Std Dev: {df['Final_Size'].std():.2f}\n\n")

            #     f.write("Size Difference (Final - Initial):\n")
            #     f.write(f"  Mean: {differences.mean():.2f}\n")
            #     f.write(f"  Median: {differences.median():.2f}\n")
            #     f.write(f"  Std Dev: {differences.std():.2f}\n\n")

            #     grew = (differences > 0).sum()
            #     shrank = (differences < 0).sum()
            #     unchanged = (differences == 0).sum()

            #     f.write("Change Categories:\n")
            #     f.write(f"  Grew: {grew} ({grew/len(df)*100:.1f}%)\n")
            #     f.write(f"  Shrank: {shrank} ({shrank/len(df)*100:.1f}%)\n")
            #     f.write(f"  Unchanged: {unchanged} ({unchanged/len(df)*100:.1f}%)\n")
            # Scatter Plot colorido por linguagem
            plt.figure(figsize=(16, 10))

            # Pegar linguagens únicas
            languages = df['Language'].unique()

            # Gerar paleta de cores
            import matplotlib.cm as cm
            colors = cm.tab20(range(len(languages)))

            # Plotar cada linguagem com cor diferente
            for i, lang in enumerate(languages):
                lang_df = df[df['Language'] == lang]
                plt.scatter(lang_df['Initial_Size'], lang_df['Final_Size'],
                            label=lang, alpha=0.6, s=50, color=colors[i], edgecolors='black', linewidth=0.5)

            # Adicionar linha diagonal (y=x)
            max_val = max(df['Initial_Size'].max(), df['Final_Size'].max())
            min_val = min(df['Initial_Size'].min(), df['Final_Size'].min())
            plt.plot([min_val, max_val], [min_val, max_val], 'k--', linewidth=2,
                    label='No Change (y=x)', alpha=0.7, zorder=0)

            plt.xlabel('Initial Size (LOC)', fontsize=12)
            plt.ylabel('Final Size (LOC)', fontsize=12)
            plt.title('Initial vs Final Size by Language\n(Above line = Growth, Below line = Reduction)', fontsize=14)
            plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=9, ncol=1)
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            plt.savefig(path.join(folder_path, 'scatter_by_language.png'), dpi=150, bbox_inches='tight')
            plt.close()

            # Versão com escala logarítmica (útil se houver grande variação de tamanhos)
            plt.figure(figsize=(16, 10))

            for i, lang in enumerate(languages):
                lang_df = df[df['Language'] == lang]
                plt.scatter(lang_df['Initial_Size'], lang_df['Final_Size'],
                            label=lang, alpha=0.6, s=50, color=colors[i], edgecolors='black', linewidth=0.5)

            # Linha diagonal
            plt.plot([min_val, max_val], [min_val, max_val], 'k--', linewidth=2,
                    label='No Change (y=x)', alpha=0.7, zorder=0)

            plt.xscale('log')
            plt.yscale('log')
            plt.xlabel('Initial Size (LOC) - Log Scale', fontsize=12)
            plt.ylabel('Final Size (LOC) - Log Scale', fontsize=12)
            plt.title('Initial vs Final Size by Language (Log Scale)\n(Above line = Growth, Below line = Reduction)', fontsize=14)
            plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=9, ncol=1)
            plt.grid(True, alpha=0.3, which='both')
            plt.tight_layout()
            plt.savefig(path.join(folder_path, 'scatter_by_language_log.png'), dpi=150, bbox_inches='tight')
            plt.close()

if __name__ == "__main__":
        df = get_mean_and_median_by_project()
        generate_graphs_initial_x_final(df, "project", "project")

        df = get_mean_and_median_by_language()

        print(f"\nTotal de arquivos diferentes: {len(df)}")
        print(f"Total de linguagens: {df['Language'].nunique()}")
        print(f"Total de projetos: {df['Project'].nunique()}")

        generate_graphs_initial_x_final(df, "language", "language")
        generate_graphs_initial_x_final(df, "all", "all")

        print("That's all, folks!")
