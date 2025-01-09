import os
import pandas as pd

# FUNCTION =============================================================================================================

def read_csv_files(directory:str, delimiter:str='|', add_columns:bool=False)->list:
    """
    Reads CSV files from a given directory and its subdirectories.

    Args:
        directory (str): The directory to read CSV files from.
        delimiter (str): The delimiter used in the CSV files. Default is '|'.
        add_columns (bool): Whether to add 'owner' and 'project' columns based on the filename. Default is False.

    Returns:
        list: A list of pandas DataFrames containing the data from the CSV files.
    """
    data_frames = []
    for language in os.listdir(directory):
        language_dir = os.path.join(directory, language)
        if os.path.isdir(language_dir):
            for filename in os.listdir(language_dir):
                if filename.endswith(".csv"):
                    file_path = os.path.join(language_dir, filename)
                    df = pd.read_csv(file_path, engine='python', delimiter=delimiter)
                    if add_columns:
                        owner, project = filename.split('~')[0], filename.split('~')[1].replace('.csv', '')
                        df['owner'] = owner
                        df['project'] = project
                    data_frames.append(df)
    return data_frames

def filter_data(combined_data_01:pd.DataFrame, data_02:pd.DataFrame)->pd.DataFrame:
    """
    Filters the combined data based on the 'percentil 99' and 'code' columns.

    Args:
        combined_data_01 (DataFrame): The first combined data DataFrame.
        data_02 (DataFrame): The second data DataFrame containing percentiles.

    Returns:
        DataFrame: The filtered DataFrame.
    """
    filtered_data = combined_data_01.merge(data_02, on='language')
    filtered_data = filtered_data[filtered_data['percentil 99'] > filtered_data['code']]
    
    return filtered_data.drop(columns=[
        'percentil 99', '#', 'percentil 25', 'percentil 50', 
        'percentil 75', 'percentil 90', 'percentil 95', 
        'percentil 97', 'percentil 98', 'comment', 'blank'
    ])

def sort_rows(filtered_data:pd.DataFrame, combined_data_06:pd.DataFrame)->pd.DataFrame:
    """
    Sorts rows based on the 'language', 'owner', and 'project' columns.

    Args:
        filtered_data (DataFrame): The filtered data DataFrame.
        combined_data_06 (DataFrame): The combined data DataFrame from the sixth output.

    Returns:
        DataFrame: The sorted DataFrame.
    """
    sorted_rows = []
    for _, row in combined_data_06.iterrows():
        language = row['language']
        owner = row['owner']
        project = row['project']
        small_files = int(row['small files p/ project'])
        files_missing = int(row['files missing'])
        avaliable_files = int(row['files available'])
        
        matching_rows = filtered_data[
            (filtered_data['language'] == language) &
            (filtered_data['owner'] == owner) &
            (filtered_data['project'] == project)
        ]
        
        if files_missing != 0:
            sorted_rows.append(matching_rows.sample(n=avaliable_files))
        else:
            sorted_rows.append(matching_rows.sample(n=small_files))
    
    return pd.concat(sorted_rows, ignore_index=True)

def save_dataframe(df:pd.DataFrame, output_dir:str)->None:
    """
    Saves the DataFrame to CSV files in the specified output directory.

    Args:
        df (DataFrame): The DataFrame to save.
        output_dir (str): The directory to save the CSV files in.
    """
    starred_projects_path = './src/_00/input/450_Starred_Projects.csv'
    starred_projects = pd.read_csv(starred_projects_path)
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    for (owner, project), group in df.groupby(['owner', 'project']):
        language = starred_projects[
            (starred_projects['owner'] == owner) & 
            (starred_projects['project'] == project)
        ]['main language'].values[0]
        
        language_dir = os.path.join(output_dir, language)
        if not os.path.exists(language_dir):
            os.makedirs(language_dir)
        
        filename = f"{owner}~{project}.csv"
        file_path = os.path.join(language_dir, filename)
        group.to_csv(file_path, index=False)

# MAIN =================================================================================================================

def main()->None:
    output_01_dir = './src/_01/output/'
    output_02 = './src/_02/output/percentis_by_language_filtered.csv'
    output_06_dir = './src/_06/output/'

    data_01 = read_csv_files(output_01_dir)
    data_02 = pd.read_csv(output_02)
    data_06 = read_csv_files(output_06_dir, delimiter=',', add_columns=True)
    
    combined_data_01 = pd.concat(data_01, ignore_index=True)
    combined_data_06 = pd.concat(data_06, ignore_index=True)
    
    percentis_data = pd.read_csv(output_02, delimiter=',')
    
    filtered_data_01 = filter_data(combined_data_01, percentis_data)
    sorted_filtered_data_01 = sort_rows(filtered_data_01, combined_data_06)
    
    print(sorted_filtered_data_01)
    
    output_dir = './src/_07/output/'
    save_dataframe(sorted_filtered_data_01, output_dir)

if __name__ == "__main__":
    main()
