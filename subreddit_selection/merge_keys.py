import sys
import pandas as pd
import numpy as np
from pathlib import Path
import glob

key_dir = Path().cwd() / 'subreddit_id_key'
k_path = key_dir.as_posix()

keyfiles = glob.glob("{}/*".format(k_path))
print(len(keyfiles))

_dfs = list()
c = 0
for fp in keyfiles:
    c += 1
    if c % 300 == 0:
        print('Processed {} so far'.format(c))
    _temp = pd.read_csv(fp, sep='\t', header=0)
    _dfs.append(_temp)

print(len(_dfs))

print(_dfs[0].shape)

alldfs = pd.concat(_dfs, ignore_index=True)
alldfs = alldfs.drop_duplicates()

print(alldfs.head())
print(alldfs.shape)

# export 
alldfs.to_csv('./subreddit_id_ALLkeys.tsv',sep='\t',index=False,header=True)
