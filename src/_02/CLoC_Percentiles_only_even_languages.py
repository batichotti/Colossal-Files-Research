import pandas as pd
import numpy as np
import os

input_path: str = './src/_01/output'
output_path: str = './src/_02/output'

all_dataframes = []

repositories_path = "./src/_00/input/450_Starred_Projects.csv"
repositories = pd.read_csv(repositories_path)

for i in range(len(repositories)):
    repository, language = repositories.loc[i, ['url', 'main language']]
    repo_path = f"{language}/{repository.split('/')[-2]}~{repository.split('/')[-1]}"
    file_path = os.path.join(input_path, f"{repo_path}.csv")
    dataframe = pd.read_csv(file_path, sep='|')
    dataframe['project language'] = language
    dataframe['project'] = repository.split('/')[-1]
    dataframe['owner'] = repository.split('/')[-2]
    all_dataframes.append(dataframe)

combined_dataframe = pd.concat(all_dataframes, ignore_index=True)

filtered_dataframe = combined_dataframe[combined_dataframe['language'] == combined_dataframe['project language']]
grouped = filtered_dataframe.groupby('project language')
output_dataframes = []

for lang, group in grouped:
    code_percentiles = group['code'].describe(percentiles=[0.25, 0.5, 0.75, 0.90, 0.95, 0.97, 0.98, 0.99]).transpose()
    
    output_dataframe = pd.DataFrame({
        'language': lang,
        '#': [len(group)],
        'percentil 25': [np.ceil(code_percentiles['25%'])],
        'percentil 50': [np.ceil(code_percentiles['50%'])],
        'percentil 75': [np.ceil(code_percentiles['75%'])],
        'percentil 90': [np.ceil(code_percentiles['90%'])],
        'percentil 95': [np.ceil(code_percentiles['95%'])],
        'percentil 97': [np.ceil(code_percentiles['97%'])],
        'percentil 98': [np.ceil(code_percentiles['98%'])],
        'percentil 99': [np.ceil(code_percentiles['99%'])]
    })
    
    output_dataframes.append(output_dataframe)

os.makedirs(output_path, exist_ok=True)

final_output_dataframe = pd.concat(output_dataframes, ignore_index=True)
final_output_dataframe.to_csv(f'{output_path}/percentis_even_languages.csv', index=False)

filtered_languages = ["C", "C#", "C++", "Dart", "Go", "Java", "JavaScript", "Kotlin", "Lua", "Objective-C", "PHP", "Python", "Ruby", "Rust", "Swift", "TypeScript"]
output_filtered = final_output_dataframe[final_output_dataframe['language'].isin(filtered_languages)]

output_filtered.to_csv(f'{output_path}/percentis_even_languages_filtered.csv', index=False)
