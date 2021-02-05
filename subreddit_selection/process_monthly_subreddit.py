import sys
import pandas as pd
import numpy as np
import glob, os, re
import csv
import time
from pathlib import Path
import multiprocessing as mp


def filter_subreddits_list(id):
    sub_df = grouped_df.loc[grouped_df.subreddit_id == id]
    num_unique_authors = sub_df.author_nunique.values.tolist()
    #num_comments_monthly = sub_df.id_count.values.tolist()
    years = set(sub_df.year.values.tolist())
    y_filter = {2018,2019,2020}

    if all(i <= 100 for i in num_unique_authors) and (years == y_filter):
        return id
        #print(years,y_filter,type(years),type(y_filter))
        #print(tracker, id)


# paths
comments_dir = Path().cwd() / 'out_comments.tsv'
c_path = comments_dir.as_posix()
c_files = glob.glob("{}/*.csv".format(c_path))

all_keys = './subreddit_id_ALLkeys.tsv'
keys_df = pd.read_csv(all_keys, sep='\t', header=0)

# process comments first
_pds = list()
counter = 0
for f in c_files:
    counter += 1
    dtypes = {'subreddit':np.str,'Year':np.int16,'Month':np.int16,'id_count':np.int64,'author_nunique':np.int64,'del_auth_sum':np.int64}
    _temp = pd.read_csv(f,sep="\t", header=0, dtype=dtypes)
    _pds.append(_temp)

print(counter)

# concat dfs
df = pd.concat(_pds, ignore_index=True)
df = df.rename(columns={'Year':'year', 'Month':'month'})
df = df.merge(keys_df, left_on='subreddit', right_on='subreddit')
df['duplicate'] = df.duplicated(subset=['subreddit','year','month'],keep=False)

print(set(df.loc[df.duplicate == True].subreddit.values.tolist()))

# group by
agg = {'id_count':'sum','author_nunique':'sum','del_auth_sum':'sum'}
grouped_df = df.groupby(by=["subreddit_id","year","month"]).agg(agg).reset_index()
grouped_df = grouped_df.merge(keys_df, left_on='subreddit_id', right_on='subreddit_id')
y_df = grouped_df.groupby(by=["subreddit_id"]).agg({'year':'nunique'}).reset_index().rename(columns={'year':'year_nunique'})
m_df = grouped_df.groupby(by=["subreddit_id"]).agg({'month':'count'}).reset_index().rename(columns={'month':'month_count'})
y_df = y_df.merge(m_df,left_on='subreddit_id', right_on='subreddit_id')

grouped_df = grouped_df.merge(y_df, left_on='subreddit_id', right_on='subreddit_id')

# get the monthly data per
subreddit_ids = grouped_df.subreddit_id.values.tolist()
#print( len(subreddit_ids), len(list(set(subreddit_ids))) )

# TODO histogram of all average monthly contributors (comments)
#g_m_df = grouped_df.groupby(by=["subreddit_id"]).agg({'month':'count'}).reset_index().rename(columns={'month':'s_month_count'})
#grouped_df = grouped_df.merge(g_m_df,left_on='subreddit_id', right_on='subreddit_id')

#g_avg = grouped_df.groupby(by=["subreddit_id"]).agg({'author_nunique':'mean'}).reset_index().rename(columns={'author_nunique':'avg_auth_unique'})
#g_avg = g_avg.merge(keys_df, left_on='subreddit_id', right_on='subreddit_id')
#g_avg = g_avg.merge(g_m_df, left_on='subreddit_id', right_on='subreddit_id')

#print(g_avg)

#g_avg.to_csv('./all_subreddits.tsv',sep='\t',index=False,header=True)


# small only
small_df = grouped_df
#small_df = grouped_df.loc[ grouped_df.author_nunique <= 100]
#small_df = small_df.loc[ small_df.author_nunique >= 5 ]
small_df = small_df.loc[ small_df.year_nunique >= 3 ]
small_df = small_df.loc[ small_df.month_count >= 36 ]

#print(small_df.sort_values(by=['month_count','subreddit_id'],ascending=True).head(50))
#print(small_df.head(50))
#print(small_df.head(80).tail(31))
#input('continue?')

#s_m_df = small_df.groupby(by=["subreddit_id"]).agg({'month':'count'}).reset_index().rename(columns={'month':'s_month_count'})
#small_df = small_df.merge(s_m_df,left_on='subreddit_id', right_on='subreddit_id')

#print(small_df.sort_values('month_count',ascending=True).head(50))
#print(small_df.head(50))
#input('continue?')

#small_df = small_df.loc[ small_df.s_month_count >= 36 ]

#print(small_df.sort_values(by=['subreddit',"year","month"]).tail(50))
#print( len( list(set(small_df.subreddit_id.values.tolist() )) ) )

avg = small_df.groupby(by=["subreddit_id"]).agg({'author_nunique':'mean'}).reset_index().rename(columns={'author_nunique':'avg_auth_unique'})
avg = avg.merge(keys_df, left_on='subreddit_id', right_on='subreddit_id')
#avg = avg.merge(s_m_df, left_on='subreddit_id', right_on='subreddit_id')

avg = avg.sort_values(by=['avg_auth_unique'])

avg = avg.loc[ avg.avg_auth_unique <= 100]
avg = avg.loc[ avg.avg_auth_unique >= 5]

print(avg)
print(len(avg))

avg.to_csv('./filtered_avg_small_subreddits.tsv',sep='\t',index=False,header=True)

#stime = time.time()
#cpus = mp.cpu_count()
#pool = mp.Pool(cpus)
#filtered_subreddits = pool.map(filter_subreddits_list, [x for x in subreddit_ids])
#print("Processed. Took {} seconds.".format(time.time() - stime))
#print("The filtered subreddits: {}".format(len( filtered_subreddits )))

# output
#with open('filtered_subreddits.txt', 'w') as f:
#    for x in filtered_subreddits:
#        f.write('{}\n'.format(x))
