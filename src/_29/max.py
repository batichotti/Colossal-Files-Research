import pandas as pd
from os import makedirs

SEPARATOR = '|'

# Setup
input_path:str = "./src/_29/input/"
output_path:str = "./src/_29/output/"

repositories_path:str = "./src/_00/input/450-linux-pytorch.csv"
cloc_path:str = "./src/_01/output/"
large_files_path:str = "./src/_03/output/"
large_files_total_per_language_path:str = "./src/_05/output/#large_files.csv"

repositories:pd.DataFrame = pd.read_csv(repositories_path)
large_files_total_per_language:pd.DataFrame = pd.read_csv(large_files_total_per_language_path)

# script
large_total = []
large_percentage_total = []

for i in range(len(repositories)):
    # getting repository information
    repository, language = repositories.loc[i, ['url', 'main language']]

    # makedirs(f"{output_path}{language}", exist_ok=True)
    repo_path: str = f"{language}/{repository.split('/')[-2]}~{repository.split('/')[-1]}"
    print(repo_path)

    repository_df: pd.DataFrame = pd.read_csv(f"{cloc_path}{repo_path}.csv", sep=SEPARATOR)
    repository_large_df: pd.DataFrame = pd.read_csv(f"{large_files_path}{repo_path}.csv", sep=SEPARATOR)

    repository_total: int = len(repository_df)
    repository_large_total: int = len(repository_large_df)

    large_total.append(
        {
            "project": [repo_path],
            "large": [repository_large_total],
            "total": [repository_total]
        }
        )
    large_percentage_total.append(
        {
            "project": [repo_path],
            "percentage": [(repository_large_total/repository_total)*100],
            "large": [repository_large_total],
            "total": [repository_total]
        }
    )

large_total = sorted(large_total, key=lambda x: x["large"][0], reverse=True)
large_percentage_total = sorted(large_percentage_total, key=lambda x: x["percentage"][0], reverse=True)

result: dict = {
    "Position": [],
    "Project Max #large": [],
    "Max #large": [],
    "Max #total": [],
    "Project Max %%#large": [],
    "Max %%large": [],
    "Max %%#large": [],
    "Max %%#total": [],
}

output_quantity = 5  # Adjust this value to show the top X results
for i in range(output_quantity):
    result["Position"].append(i + 1)
    result["Project Max #large"].append(large_total[i]["project"][0])
    result["Max #large"].append(large_total[i]["large"][0])
    result["Max #total"].append(large_total[i]["total"][0])
    result["Project Max %%#large"].append(large_percentage_total[i]["project"][0])
    result["Max %%large"].append(large_percentage_total[i]["percentage"][0])
    result["Max %%#large"].append(large_percentage_total[i]["large"][0])
    result["Max %%#total"].append(large_percentage_total[i]["total"][0])


makedirs(f"{output_path}", exist_ok=True)
pd.DataFrame(result).to_csv(f"{output_path}result.csv")
