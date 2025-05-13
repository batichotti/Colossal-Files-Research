import pandas as pd
from os import makedirs, path
import datetime

SEPARATOR = ';'

# SETUP ================================================================================================================

input_path:str = "./src/_35/input/"
output_path = "./src/_35/output/"

repositories_path:str = "./src/_00/input/avalonia.csv"
# repositories_path:str = "./src/_00/input/450-linux-pytorch.csv"
language_white_list_path: str = "./src/_12/input/white_list.csv"
percentil_path: str = "./src/_02/output/percentis_by_language_filtered.csv"
large_filtered_path:str = "./src/_34/output/large/"
small_filtered_path:str = "./src/_34/output/small/"

language_white_list_df = pd.read_csv(language_white_list_path)
percentil_df = pd.read_csv(percentil_path)

repositories:pd.DataFrame = pd.read_csv(repositories_path)
# print(repositories)

# ======================================================================================================================

for i, row in repositories.iterrows():
    repo_url: str = row['url']
    language: str = row['main language']
    repo_name: str = repo_url.split('/')[-1]
    repo_owner: str = repo_url.split('/')[-2]
    repo_path: str = f"{language}/{repo_owner}~{repo_name}"
    print(repo_path)

    makedirs(f"{output_path}large/{repo_path}", exist_ok=True)
    if path.exists(f"{large_filtered_path}{repo_path}.csv"):
        large_files_data = pd.read_csv(f"{large_filtered_path}{repo_path}.csv", sep=SEPARATOR)
        if not large_files_data.empty:
            larges_grouped = large_files_data.groupby("File Name")
            for file_name, large_file_history in larges_grouped:
                # renaming columns
                large_file_history.rename(columns={
                    'Lines Of Code (nloc)': 'nLoc',
                    'Committer Commit Date': 'Date',
                    'Number of Files': '#Files on commit'
                }, inplace=True)


                # large_file_history['Size Change']
                large_file_history['Size Change'] = large_file_history['Lines Balance'].apply(
                    lambda x: 'Grew' if x > 0 else 'Decreased' if x < 0 else 'Equal'
                )


                # large_file_history['Lines Balance (nLoc)']
                large_file_history['nLoc'] = pd.to_numeric(large_file_history['nLoc'], errors='coerce')
                large_file_history['Lines Balance'] = large_file_history['nLoc'].diff().fillna(0)
                large_file_history['nLoc'] = large_file_history['nLoc'].fillna('not calculated')


                # large_file_history['Is Large?']
                large_file_history['Extension'] = large_file_history['File Name'].apply(lambda x: x.split(".")[-1])

                large_file_history = large_file_history.merge(
                    language_white_list_df[['Extension', 'Language']],
                    on='Extension',
                    how='left'
                ).drop(columns=['Extension'])

                percentil_99 = percentil_df.set_index('language')['percentil 99']
                large_file_history['nLoc'] = pd.to_numeric(large_file_history['nLoc'], errors='coerce')
                large_file_history['Is Large?'] = large_file_history.apply(
                    lambda x: True if pd.notna(x['nLoc']) and x['nLoc'] >= percentil_99.get(x['Language'], 0) else False,
                    axis=1
                )
                large_file_history['nLoc'] = large_file_history['nLoc'].fillna('not calculated')


                # large_file_history['Swapped Classification?']
                large_file_history['Swapped Classification?'] = large_file_history['Is Large?'].ne(
                    large_file_history['Is Large?'].shift(fill_value=False)
                )

                # sorting cols
                large_file_history = large_file_history[[
                    'File Name', 'Change Type',
                    'nLoc', 'Lines Balance', 'Size Change', 'Is Large?', 'Date',
                    'Swapped Classification?',
                    'Complexity', 'Methods', 'Tokens',
                    'Hash', '#Files on commit', 'Committer Email', 'Author Email'
                ]]

                # saving file
                large_file_history.to_csv(f"{output_path}large/{repo_path}/{file_name}.csv", sep=SEPARATOR, index=False)

                # with pd.option_context('display.max_rows', None, 'display.max_columns', None):
                #     input(large_file_history.to_string(index=False))
