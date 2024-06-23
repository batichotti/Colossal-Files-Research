import pandas as pd
import os

path_01_output = 'src/_01/output'
all_output_dataframes = []

for language_dir in os.listdir(path_01_output):
    language_dir_path = os.path.join(path_01_output, language_dir)
    for file in os.listdir(language_dir_path):
        file_path = os.path.join(language_dir_path, file)
        dataframe = pd.read_csv(file_path, sep='|')
        
        # Agrupar os dados por 'language' antes de calcular os percentis
        grouped = dataframe.groupby('language')
        
        for lang, group in grouped:
            code_percentiles = group['code'].describe(percentiles=[0.25, 0.5, 0.75, 0.90, 0.95, 0.97, 0.98, 0.99]).transpose()
            
            output_dataframe = pd.DataFrame({
                'project language': language_dir,
                'project': file.split('~')[1],
                'owner': file.split('~')[0],
                'code language': lang,
                '#': len(group),
                'percentil 25': [code_percentiles['25%']],
                'percentil 50': [code_percentiles['50%']],
                'percentil 75': [code_percentiles['75%']],
                'percentil 90': [code_percentiles['90%']],
                'percentil 95': [code_percentiles['95%']],
                'percentil 97': [code_percentiles['97%']],
                'percentil 98': [code_percentiles['98%']],
                'percentil 99': [code_percentiles['99%']]
            })
            all_output_dataframes.append(output_dataframe)

final_output_dataframe = pd.concat(all_output_dataframes, ignore_index=True)
output_file_path = os.path.join('src/_02/output', 'out.csv')
final_output_dataframe.to_csv(output_file_path, index=False)
