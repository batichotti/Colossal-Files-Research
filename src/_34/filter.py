import pandas as pd
from os import makedirs, path
import datetime

SEPARATOR = '|'

# SETUP ================================================================================================================

input_path:str = "./src/_34/input/"
output_path = "./src/_34/output/"

repositories_path:str = "./src/_00/input/450-linux-pytorch.csv"
large_files_path:str = "./src/_03/output/"
large_drill_path:str = "./src/_10/output/large_files/"
small_files_path:str = "./src/_07/output/"
small_drill_path:str = "./src/_10/output/small_files/"

repositories:pd.DataFrame = pd.read_csv(repositories_path)
# print(repositories)

# ======================================================================================================================

for i in range(len(repositories)):
    # getting repository information
    repository, language = repositories.loc[i, ['url', 'main language']]

    repo_path = f"{language}/{repository.split('/')[-2]}~{repository.split('/')[-1]}"
    print(repo_path)

    makedirs(f"{output_path}large/{language}", exist_ok=True)
    if path.exists(f"{large_files_path}{repo_path}.csv"):
        large_file_list = pd.read_csv(f"{large_files_path}{repo_path}.csv", sep=SEPARATOR)
        if not large_file_list.empty:
            large_drill = pd.read_csv(f"{large_drill_path}{repo_path}.csv", sep=SEPARATOR)

            large_drill['Local File PATH Old'] = large_drill['Local File PATH Old'].fillna('file deleted')

            # Add "File Extension" column equal to "language" in large_file_list
            large_file_list['File Extension'] = large_file_list['language']

            # Add "Large Percentile" column equal to "code" in large_file_list
            large_file_list['Large Percentile'] = large_file_list['code']

            large_file_list['path'] = large_file_list['path'].apply(lambda x: "/".join(x.split("/")[6:]))

            # Filter large_drill based on conditions
            large_drill = large_drill[
                large_drill['Local File PATH Old'].isin(large_file_list['path']) |
                large_drill['Local File PATH New'].isin(large_file_list['path'])
            ]

            large_drill["Lines Balance"] = large_drill["Lines Added"] - large_drill["Lines Deleted"]

            large_drill = large_drill.drop(columns=['Project Name', 'Local Commit PATH', 'Merge Commit', 'Message', 'Author Commit Date', 'Author Commit Timezone'])

            # Reorder columns to place "Lines Balance" after "Lines Deleted"
            columns = list(large_drill.columns)
            columns.insert(columns.index('Lines Deleted') + 1, columns.pop(columns.index('Lines Balance')))
            large_drill = large_drill[columns]
            # Adjust and sort by "Committer Commit Date"
            large_drill['Committer Commit Date'] = large_drill['Committer Commit Date'].apply(
                lambda x: x[:-3] + x[-2:]  # Remove the ':' from the offset (+02:00 → +0200)
            )
            large_drill['Committer Commit Date'] = large_drill['Committer Commit Date'].apply(
                lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S%z').astimezone(datetime.timezone.utc)
            )

            # Sort large_drill by "File Name" and then by "Committer Commit Date"
            large_drill = large_drill.sort_values(by=["File Name", "Committer Commit Date"])

            large_drill.to_csv(f"{output_path}large/{repo_path}.csv", sep=";", index=False)
    
    makedirs(f"{output_path}small/{language}", exist_ok=True)
    if path.exists(f"{small_files_path}{repo_path}.csv"):
        small_file_list = pd.read_csv(f"{small_files_path}{repo_path}.csv", sep=SEPARATOR)
        if not small_file_list.empty:
            small_drill = pd.read_csv(f"{small_drill_path}{repo_path}.csv", sep=SEPARATOR)

            small_drill['Local File PATH Old'] = small_drill['Local File PATH Old'].fillna('file deleted')

            # Add "File Extension" column equal to "language" in small_file_list
            small_file_list['File Extension'] = small_file_list['language']

            # Add "Small Percentile" column equal to "code" in small_file_list
            small_file_list['Small Percentile'] = small_file_list['code']

            small_file_list['path'] = small_file_list['path'].apply(lambda x: "/".join(x.split("/")[6:]))

            # Filter small_drill based on conditions
            small_drill = small_drill[
                small_drill['Local File PATH Old'].isin(small_file_list['path']) |
                small_drill['Local File PATH New'].isin(small_file_list['path'])
            ]

            small_drill["Lines Balance"] = small_drill["Lines Added"] - small_drill["Lines Deleted"]

            small_drill = small_drill.drop(columns=['Project Name', 'Local Commit PATH', 'Merge Commit', 'Message', 'Author Commit Date', 'Author Commit Timezone'])

            # Reorder columns to place "Lines Balance" after "Lines Deleted"
            columns = list(small_drill.columns)
            columns.insert(columns.index('Lines Deleted') + 1, columns.pop(columns.index('Lines Balance')))
            small_drill = small_drill[columns]
            # Adjust and sort by "Committer Commit Date"
            small_drill['Committer Commit Date'] = small_drill['Committer Commit Date'].apply(
                lambda x: x[:-3] + x[-2:]  # Remove the ':' from the offset (+02:00 → +0200)
            )
            small_drill['Committer Commit Date'] = small_drill['Committer Commit Date'].apply(
                lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S%z').astimezone(datetime.timezone.utc)
            )

            # Sort small_drill by "File Name" and then by "Committer Commit Date"
            small_drill = small_drill.sort_values(by=["File Name", "Committer Commit Date"])

            small_drill.to_csv(f"{output_path}small/{repo_path}.csv", sep=";", index=False)
