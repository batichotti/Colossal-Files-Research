
import pandas as pd

def frmt_strp(path):
    df = pd.read_csv(path)
    df = df.drop(['owner','Project'], axis=1)

    df['owner'] = df['url'].apply(lambda x: x.split('/')[-2])
    df['project'] = df['url'].apply(lambda x: x.split('/')[-1])

    df.to_csv(f'{path}_N.csv')
# frmt_strp('src/_00/input/600_Starred_Projects.csv')

def enc_tr(path):
    df = pd.read_csv(path)
    df = df[df['status'] == 'No Main/Master branches found']

    ndf = pd.read_csv('src\_00\input\600_Starred_Projects_N.csv')
    
    ndf = ndf[(ndf['owner']==df['repo'].apply(lambda x: x.split('~')[0])and(ndf['project']==df['repo'].apply(lambda x: x.split('~')[1])))]

    ndf.to_csv(f'{path}_not_found.csv')

enc_tr('src/_00/output/enc.csv')