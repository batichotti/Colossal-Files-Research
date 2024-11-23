import pandas as pd
import os

data = []
for folder in os.listdir('./output/'):
    for arx in folder:
        if arx.endswith('.csv'):
            arx = arx[:-4]
            data.append(f'https://github.com/{arx.split('~')[0]}/{arx.split('~')[1]}')
            
df = pd.DataFrame(data)
df.to_csv('LISTA_DOS_PROJETOS_CLOCADOS.csv')
