import pyarrow as pa
import numpy as np
import pandas as pd
import argparse
import sys, os, glob
import time

def parse_args():
    parser = argparse.ArgumentParser(description='Process Reddit parquet file(s).')
    parser.add_argument('-i', '--input', help='Parquet file to input.', type=str)
    parser.add_argument('-o', '--outdir', help='Output directory.', default='processing_comments_per_subreddit', type=str)
    args = parser.parse_args()
    return(args)

if __name__ == "__main__":
    start_time = time.time()
    args = parse_args()

    outdir = './data/{}'.format(args.outdir)
    if not os.path.isdir(outdir):
        os.mkdir(outdir)

    print("Processing {}".format(args.input))

    if args.outdir == 'processing_comments_per_subreddit':
        parquet_files = glob.glob('/gscratch/comdata/output/reddit_comments_by_subreddit.parquet/*.parquet')
        parquet_path = '{}'.format(args.input)
        outname = 'out_{}.tsv'.format(args.input[62:])
    elif args.outdir == 'processing_submissions_per_subreddit':
        parquet_files = glob.glob('/gscratch/comdata/output/reddit_submissions_by_subreddit.parquet/*.parquet')
        parquet_path = '{}'.format(args.input)
        outname = 'out_{}.tsv'.format(args.input[65:])
    else:
        print("Error in outdir choice: Should be <processing_comments_by_subreddit> or <processing_submissions_per_subreddit>\nGot{}.".format(args.outdir))
        sys.exit()
 
    df = pd.read_parquet(parquet_path,
                         columns = ['id','CreatedAt','author','subreddit','subreddit_type','subreddit_id'])

    date_filter = pd.Timestamp(2015, 1, 1, 0)
    df = df.loc[ df['CreatedAt'] >= date_filter ]
    df['year'] = df['CreatedAt'].dt.year
    df['month'] = df['CreatedAt'].dt.month
    df = df[pd.notna(df.subreddit)] # Removes ads
    df = df.drop_duplicates()
    df['del_auth'] = df['author']
    df.loc[df['author'] != '[deleted]', 'del_auth'] = 0
    df.loc[df['author'] == '[deleted]', 'del_auth'] = 1

    id_to_subreddit_key_df = df.loc[:,df.columns.isin(['subreddit_id','subreddit'])]
    id_to_subreddit_key_df = id_to_subreddit_key_df.drop_duplicates()

    agg_dict = {'id':'count','author':'nunique','del_auth':'sum'}
    df_auth = df.groupby(['subreddit_id', 'year', 'month']).agg(agg_dict).reset_index()
    df_auth = df_auth.rename(columns={'id':'id_count','author':'author_nunique','del_auth':'del_auth_sum'})

    # number of comments/submissions per month-year
    # number of [deleted] authors per month-year
    # number of unique authors, [deleted] as 1, per month-year

    outpath = '{}/{}'.format(outdir,outname)
    df_auth.to_csv(outpath,sep='\t',index=False,header=True)
    key_outpath = 'subreddit_id_key/key_{}'.format(outname)
    id_to_subreddit_key_df.to_csv(key_outpath,sep='\t',index=False,header=True)

    print(' > Output: {}\n > Took {} seconds.'.format(outpath, time.time() - start_time))
