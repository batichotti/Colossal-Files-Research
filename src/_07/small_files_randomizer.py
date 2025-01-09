import os
import pandas as pd

def read_csv_files(directory):
    data_frames = []
    for language in os.listdir(directory):
        language_dir = os.path.join(directory, language)
        if os.path.isdir(language_dir):
            for filename in os.listdir(language_dir):
                if filename.endswith(".csv"):
                    file_path = os.path.join(language_dir, filename)
                    df = pd.read_csv(file_path, engine='python', delimiter='|')
                    data_frames.append(df)
    return data_frames

def main():
    output_01_dir = './src/_01/output/'
    output_03_dir = './src/_03/output/'
    output_06_dir = './src/_06/output/'

    data_01 = read_csv_files(output_01_dir)
    data_03 = read_csv_files(output_03_dir)
    data_06 = read_csv_files(output_06_dir)
    
    combined_data_01 = pd.concat(data_01, ignore_index=True)
    combined_data_03 = pd.concat(data_03, ignore_index=True)

    filtered_data_01 = combined_data_01[~combined_data_01.isin(combined_data_03.to_dict(orient='list')).all(axis=1)]

    

if __name__ == "__main__":
    main()
