import pandas as pd
from os import makedirs, path, listdir

SEPARATOR = ';'

output_path = "./src/_36/output/"

def concat(folder:str) -> bool:
    df_list = []
    if path.exists(folder):
        languages = listdir(folder)
        for language in languages:
            projects = listdir(path.join(folder, language))
            for project in projects:
                df = pd.read_csv(path.join(folder, language, project))
                df_list.append(df)
    if df_list:
        df_output = pd.concat(df_list, ignore_index=True)
        df_output.to_csv(folder + "_concatenated.csv", sep=SEPARATOR, index=False)
        return True
    return False


concat(path.join(output_path, "large"))
concat(path.join(output_path, "small"))
