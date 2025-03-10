import pandas as pd
from os import makedirs
import matplotlib.pyplot as plt
from adjustText import adjust_text

SEPARATOR = '|'

# Setup
input_path:str = "./src/_09/input/"
output_path:str = "./src/_09/output/"

repositories_path:str = "./src/_00/input/450_Starred_Projects.csv"
cloc_path:str = "./src/_01/output/"
large_files_path:str = "./src/_03/output/"

repositories:pd.DataFrame = pd.read_csv(repositories_path)

# script
language_stats = {}

# buisness logic
deny_languages = [
    'Markdown',
    'INI',
    'diff',
    'CUDA',
    'XML',
    'SVG',
    'SQL',
    'HTML',
    'CSS',
    'Text',
    'YAML',
    'JSON',
    'Svelte',
    'Gradle',
    'TOML',
    'Scheme',
    'Bourne Shell', # Objective-C/MustangYM~WeChatExtension-ForMac
    'Fish Shell',
    'GLSL',
    'QML',
    'Handlebars'
]

for i in range(len(repositories)):
    save_df:pd.DataFrame = pd.DataFrame(columns=[
        'Linguagem',
        'Projeto',
        'Linguagem Majoritaria',
        '# Arquivos Grandes da Linguagem Majoritaria',
        '# Total de Arquivos Grandes',
        '%'
    ])

    # getting repository information
    repository, language = repositories.loc[i, ['url', 'main language']]

    makedirs(f"{output_path}{language}", exist_ok=True)
    repo_path = f"{language}/{repository.split('/')[-2]}~{repository.split('/')[-1]}"

    # getting a file total per language
    repository_files_df:pd.DataFrame = pd.read_csv(f"{cloc_path}/{repo_path}.csv", sep=SEPARATOR)
    repository_files_df:pd.Series = repository_files_df.groupby('language').size()
    # filtering out languages in the deny list
    repository_files_df = repository_files_df[~repository_files_df.index.isin(deny_languages)]
    # pegando o elemento com maior número de um pd.Series
    major_language = repository_files_df.idxmax() if not repository_files_df.empty else 'Unknown'

    # getting a large file total per language
    large_files_df:pd.DataFrame = pd.read_csv(f"{large_files_path}/{repo_path}.csv", sep=SEPARATOR)
    large_files_df:pd.Series = large_files_df.groupby('language').size()
    if major_language in large_files_df.index:
        large_files_major_language:int = large_files_df[major_language]
    else:
        large_files_major_language:int = 0
    total_large_files:int = large_files_df.sum()
    percentage:float = (large_files_major_language / total_large_files) * 100 if total_large_files > 0 else 0
    
    print(repo_path)
    #print(repository_files_df)
    #print(f"Linguagem Majoritaria: {major_language}")
    #print(f"Arquivos Grandes da Linguagem Majoritaria: {large_files_major_language}")
    #print(f"Total de Arquivos Grandes: {total_large_files}")
    #print(f"Porcentagem de Arquivos Grandes da Linguagem Majoritaria: {percentage:.2f}%")
    #input()

    makedirs(f"{output_path}{language}", exist_ok=True)
    
    save_df.loc[len(save_df)] = [language, repository.split('/')[-1], major_language, large_files_major_language, total_large_files, percentage]
    save_df.to_csv(f"{output_path}{language}/{repository.split('/')[-2]}~{repository.split('/')[-1]}.csv", sep=SEPARATOR, index=False)

    # Update language stats
    if language not in language_stats or total_large_files > language_stats[language]['total_large_files']:
        language_stats[language] = {
            'repository': repository.split('/')[-1],
            'major_language': major_language,
            'large_files_major_language': large_files_major_language,
            'total_large_files': total_large_files,
            'percentage': percentage
        }


# Tabela ===========================================================================================================

# Save the project with the most large files for each language
summary_df = pd.DataFrame(columns=[
    'Linguagem',
    'Projeto',
    'Linguagem Majoritaria',
    '# Arquivos Grandes da Linguagem Majoritaria',
    '# Total de Arquivos Grandes',
    '%'
])

for language, stats in language_stats.items():
    summary_df.loc[len(summary_df)] = [
        language,
        stats['repository'],
        stats['major_language'],
        stats['large_files_major_language'],
        stats['total_large_files'],
        stats['percentage']
    ]

summary_df.to_csv(f"{output_path}summary.csv", sep=SEPARATOR, index=False)

# Grafico ===========================================================================================================

# Create a DataFrame to store the total large files count for each project
project_large_files_df = pd.DataFrame(columns=['Projeto', '# Total de Arquivos Grandes'])

for i in range(len(repositories)):
    repository = repositories.loc[i, 'url']
    language = repositories.loc[i, 'main language']
    repo_path = f"{language}/{repository.split('/')[-2]}~{repository.split('/')[-1]}"
    
    large_files_df = pd.read_csv(f"{large_files_path}/{repo_path}.csv", sep=SEPARATOR)
    total_large_files = large_files_df.shape[0]
    
    project_large_files_df.loc[len(project_large_files_df)] = [repository.split('/')[-1], total_large_files]

# Count the number of projects for each total large files count
project_count_df = project_large_files_df['# Total de Arquivos Grandes'].value_counts().reset_index()
project_count_df.columns = ['# Total de Arquivos Grandes', 'Quantidade de Projetos']

# Cria figura maior
plt.figure(figsize=(10, 6))
ax = plt.gca()

# Scatter plot
plt.scatter(
    project_count_df['# Total de Arquivos Grandes'], 
    project_count_df['Quantidade de Projetos'], 
    label='Projetos',
    zorder=3,
    s=25  # Tamanho dos pontos
)

# Linha de tendência
plt.plot(
    project_count_df['# Total de Arquivos Grandes'], 
    project_count_df['Quantidade de Projetos'], 
    linestyle='--', 
    color='orange', 
    alpha=0.7,
    linewidth=1.05,
    label='Linha de Tendência'
)

texts = []
top_projects = project_large_files_df.nlargest(15, '# Total de Arquivos Grandes')

# Parâmetros de espalhamento
DIRECTIONAL_OFFSET = {
    'right': 0.2,
    'left': -0.2,
    'up': 0.3,
    'down': -0.1
}

for idx, row in top_projects.iterrows():
    x_val = row['# Total de Arquivos Grandes']
    y_val = project_count_df.loc[
        project_count_df['# Total de Arquivos Grandes'] == x_val, 
        'Quantidade de Projetos'
    ].values[0]
    
    # Offset direcional alternado
    offset_x = DIRECTIONAL_OFFSET['right'] if idx % 2 else DIRECTIONAL_OFFSET['left']
    offset_y = DIRECTIONAL_OFFSET['up'] if idx % 3 else DIRECTIONAL_OFFSET['down']
    
    texts.append(plt.text(
        x_val + offset_x,
        y_val + offset_y,
        row['Projeto'],
        fontsize=10,
        ha='center',
        va='center',
        rotation=25 if idx % 2 else -25,  # Rotação alternada
        bbox=dict(
            facecolor='white', 
            alpha=0.95, 
            edgecolor='silver',
            boxstyle='round,pad=0.3'
        )
    ))

# Ajuste otimizado para espalhamento
adjust_text(
    texts,
    arrowprops=dict(
        arrowstyle="fancy,head_length=0.4,head_width=0.4",
        color='dimgray', 
        lw=0.8,
        alpha=0.7,
        connectionstyle="arc3,rad=0.3"
    ),
    expand_text=(1.5, 2.0),  # Mais expansão
    expand_points=(1.3, 1.8),
    force_text=(0.8, 1.2),    # Força aumentada
    force_points=(0.6, 0.9),
    ax=ax,
    precision=0.001,
    lim=2000  # Mais iterações
)

# Configurações finais
plt.title('Distribuição de Projetos por Arquivos Grandes', pad=25, fontsize=14)
plt.xlabel('Total de Arquivos Grandes', fontsize=12)
plt.ylabel('Quantidade de Projetos', fontsize=12)
plt.legend(loc='upper right', framealpha=0.9)
plt.grid(True, linestyle=':', alpha=0.5)
plt.tight_layout()
plt.show()
