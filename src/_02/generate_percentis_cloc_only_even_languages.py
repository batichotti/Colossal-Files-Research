import pandas as pd
import numpy as np
import os

path_01_output = 'src/_01/output'
all_dataframes = []

for language_dir in os.listdir(path_01_output):
    language_dir_path = os.path.join(path_01_output, language_dir)
    for file in os.listdir(language_dir_path):
        file_path = os.path.join(language_dir_path, file)
        dataframe = pd.read_csv(file_path, sep='|')
        dataframe['project language'] = language_dir
        dataframe['project'] = file.split('~')[1]
        dataframe['owner'] = file.split('~')[0]
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

final_output_dataframe = pd.concat(output_dataframes, ignore_index=True)

output_file_path = os.path.join('src/_02/output', 'percentis_even_languages.csv')
final_output_dataframe.to_csv(output_file_path, index=False)

filtered_languages = ["C", "C#", "C++", "Dart", "Elixir", "Go", "Haskell", "Java", "JavaScript", "Kotlin", "Lua", "Objective-C", "Perl", "PHP", "Python", "Ruby", "Rust", "Scala", "Swift", "TypeScript"]
output_filtered = final_output_dataframe[final_output_dataframe['language'].isin(filtered_languages)]
output_file_path_filtered = os.path.join('src/_02/output', 'percentis_even_languages_filtered.csv')
output_filtered.to_csv(output_file_path_filtered, index=False)
