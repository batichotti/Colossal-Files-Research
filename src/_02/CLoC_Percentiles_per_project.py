import pandas as pd
import numpy as np
import os

input_path = 'src/_01/output'
output_path = 'src/_02/output'

all_output_dataframes = []

# Loop through each language directory and read the files
for language_dir in os.listdir(input_path):
    language_dir_path = os.path.join(input_path, language_dir)
    for file in os.listdir(language_dir_path):
        file_path = os.path.join(language_dir_path, file)
        dataframe = pd.read_csv(file_path, sep='|')
        
        # Group the data by 'language' before calculating percentiles
        grouped = dataframe.groupby('language')
        
        for lang, group in grouped:
            code_percentiles = group['code'].describe(percentiles=[0.25, 0.5, 0.75, 0.90, 0.95, 0.97, 0.98, 0.99]).transpose()
            
            output_dataframe = pd.DataFrame({
                'project language': language_dir,
                'project': file.split('~')[1],
                'owner': file.split('~')[0],
                'code language': lang,
                '#': len(group),
                'percentil 25': [np.ceil(code_percentiles['25%'])],
                'percentil 50': [np.ceil(code_percentiles['50%'])],
                'percentil 75': [np.ceil(code_percentiles['75%'])],
                'percentil 90': [np.ceil(code_percentiles['90%'])],
                'percentil 95': [np.ceil(code_percentiles['95%'])],
                'percentil 97': [np.ceil(code_percentiles['97%'])],
                'percentil 98': [np.ceil(code_percentiles['98%'])],
                'percentil 99': [np.ceil(code_percentiles['99%'])]
            })
            all_output_dataframes.append(output_dataframe)

final_output_dataframe = pd.concat(all_output_dataframes, ignore_index=True)
final_output_dataframe.to_csv(f'{output_path}/percentis_by_project.csv', index=False)
