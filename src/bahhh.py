import pandas as pd

df = pd.read_csv('src/_01/input/600_Starred_Projects.csv')
df = df.drop(['owner','Project'], axis=1)

df['owner'] = df['url'].apply(lambda x: x.split('/')[-2])
df['project'] = df['url'].apply(lambda x: x.split('/')[-1])

df.to_csv('src/_01/input/600_Starred_Projects_N.csv')
