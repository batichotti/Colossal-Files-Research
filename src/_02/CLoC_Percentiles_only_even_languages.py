import pandas as pd
import numpy as np
import os

input_path = 'src/_01/output'
output_path = 'src/_02/output'

all_dataframes = []

for language_dir in os.listdir(input_path):
    language_dir_path = os.path.join(input_path, language_dir)
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
final_output_dataframe.to_csv(f'{output_path}/percentis_even_languages.csv', index=False)

filtered_languages = ["C", "C#", "C++", "Dart", "Elixir", "Go", "Haskell", "Java", "JavaScript", "Kotlin", "Lua", "Objective-C", "Perl", "PHP", "Python", "Ruby", "Rust", "Scala", "Swift", "TypeScript"]
output_filtered = final_output_dataframe[final_output_dataframe['language'].isin(filtered_languages)]
output_filtered.to_csv(f'{output_path}/percentis_even_languages_filtered.csv', index=False)
