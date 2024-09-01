import pandas as pd

input_path: str = ''
output_path: str = './src/_03/output/'

csv_node = './src/_01/output/JavaScript/nodejs~node.csv'
csv_meta = './src/_02/output/percentis_by_language_filtered.csv'

df_node = pd.read_csv(csv_node, delimiter='|')
df_meta = pd.read_csv(csv_meta)

merged_df = pd.merge(df_node, df_meta[['language', 'percentil 99']], on='language')

filtered_df = merged_df[merged_df['code'] >= merged_df['percentil 99']]

final_df = filtered_df[['path', 'owner', 'project', 'language', 'code']]

final_df.to_csv(f'{output_path}JavaScript/nodejs~node.csv', index=False)

print(final_df)

#min_code_per_language = final_df.loc[final_df.groupby('language')['code'].idxmin()]
#print(min_code_per_language)