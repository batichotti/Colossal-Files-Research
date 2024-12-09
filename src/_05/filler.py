import pandas as pd

# Caminhos para as tabelas de entrada
table1_path = './src/_05/input/percentis_by_language_filtered.csv'  # Substitua pelo caminho real
table2_path = './src/_05/output/large_files_language_counts.csv'  # Substitua pelo caminho real

# Caminho para a tabela de saída
output_path = './src/_05/output/nao_large_files.csv'

# Carregando as tabelas
table1 = pd.read_csv(table1_path)  # Tabela 1 com 'language' e '#'
table2 = pd.read_csv(table2_path)  # Tabela 2 com 'language' e '#'

# Renomeando colunas para identificar as tabelas
table1.rename(columns={'#': 'all'}, inplace=True)
table2.rename(columns={'#': 'larges'}, inplace=True)

# Realizando o merge pelas chaves primárias (language)
merged_table = pd.merge(table1, table2, on='language', how='outer')

# Calculando a diferença entre os valores
merged_table['slims'] = merged_table['all'] - merged_table['larges']

# Mantendo apenas as colunas desejadas
merged_table = merged_table[['language', 'all', 'larges', 'slims']]

# Calculando a diferença total das somas
total_count1 = merged_table['all'].sum()
total_count2 = merged_table['larges'].sum()
total_diff = total_count1 - total_count2

# Adicionando a linha TOTAL
total_row = pd.DataFrame([{
    'language': 'TOTAL',
    'all': total_count1,
    'larges': total_count2,
    'slims': total_diff
}])

# Adicionando a linha total ao final da tabela
final_table = pd.concat([merged_table, total_row], ignore_index=True)

# Salvando a tabela resultante
final_table.to_csv(output_path, index=False)
print(f"Tabela `nao_large_files` salva em: {output_path}")
