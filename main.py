import pandas as pd
import cn2an


file_list = ['A_lvr_land_A.csv', 'B_lvr_land_A.csv', 'E_lvr_land_A.csv', 'F_lvr_land_A.csv', 'H_lvr_land_A.csv']


df = pd.DataFrame()


def floor_transform(x):
    if x is not None:
        res = cn2an.transform(x)
        return int(res[:-1])
    return 0


for file in file_list:
    df = df.append(pd.read_csv(f'data/{file}'))


df = df[(df['主要用途'] == '住家用') & (df['建物型態'].str[:4] == '住宅大樓')]
df = df[df['總樓層數'].apply(floor_transform) >= 13]
df.to_csv('res.csv', index=False)
