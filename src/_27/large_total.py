import pandas as pd
from os import makedirs

SEPARATOR = '|'

# Setup
input_path:str = "./src/_27/input/"
output_path:str = "./src/_27/output/"

repositories_path:str = "./src/_00/input/450-linux-pytorch.csv"
large_files_path:str = "./src/_03/output/"

repositories:pd.DataFrame = pd.read_csv(repositories_path)

languages: dict = {}

for i in range(len(repositories)):
# getting repository information
    repository, language = repositories.loc[i, ['url', 'main language']]

    repo_path: str = f"{language}/{repository.split('/')[-2]}~{repository.split('/')[-1]}"

    print(repo_path)

    repository_df = pd.read_csv(f"{large_files_path}{repo_path}.csv", sep=SEPARATOR)

    languages[language] = languages.get(language, 0) + repository_df['language'].sum()

pd.DataFrame(list(languages.items()), columns=["Language", "File Count"]).to_csv(f"{output_path}files_total_per_language.csv", index=False)
