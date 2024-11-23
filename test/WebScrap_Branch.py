import pandas as pd
from requests import get
from bs4 import BeautifulSoup as bs


df = pd.read_csv('src\\_00\\input\\600_Starred_Projects.csv')
df['branch'] = None

for i in range(len(df)):
    url = df.loc[i]['url']
    response = get(url)

    if response.status_code == 200:
        soup = bs(response.content, 'html.parser')
        branch = soup.find('span', {'class': 'Text-sc-17v1xeu-0 bOMzPg'}).text[1:]
        print(f'{url} -> {branch}')
    else:
        print(f"Falha na requisição. Status code: {response.status_code}")
        exit(0)
    df.at[i, 'branch'] = branch

df.to_csv('src\\_00\\input\\600_Starred_Projects.csv', index=False)
