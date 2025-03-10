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

# Cria figura maior para melhor espaçamento
plt.figure(figsize=(10, 6))
ax = plt.gca()

plt.scatter(
    project_count_df['# Total de Arquivos Grandes'], 
    project_count_df['Quantidade de Projetos'], 
    label='Projetos',
    zorder=3
)

plt.plot(
    project_count_df['# Total de Arquivos Grandes'], 
    project_count_df['Quantidade de Projetos'], 
    linestyle='--', 
    color='orange', 
    alpha=0.5,
    label='Linha de Tendência'
)

texts = []
top_projects = project_large_files_df.nlargest(15, '# Total de Arquivos Grandes')

for _, row in top_projects.iterrows():
    x_val = row['# Total de Arquivos Grandes']
    y_val = project_count_df.loc[
        project_count_df['# Total de Arquivos Grandes'] == x_val, 
        'Quantidade de Projetos'
    ].values[0]
    
    texts.append(plt.text(
        x_val, 
        y_val,
        row['Projeto'],
        fontsize=9,
        ha='center',
        va='bottom',
        bbox=dict(facecolor='white', alpha=0.85, edgecolor='none', pad=2.5)  # Fundo branco (Alteração 5)
    ))

adjust_text(
    texts,
    arrowprops=dict(arrowstyle="-|>", color='gray', lw=0.5),
    expand_text=(1.2, 1.5),
    expand_points=(1.2, 1.5),
    force_text=(0.5, 0.8),
    force_points=(0.8, 0.8),
    ax=ax,
    precision=0.001
)

plt.title('Quantidade de Projetos vs. Total de Arquivos Grandes', pad=20)
plt.xlabel('Total de Arquivos Grandes')
plt.ylabel('Quantidade de Projetos')
plt.legend()
plt.grid(True, alpha=0.4, zorder=0)
plt.tight_layout()
plt.show()
