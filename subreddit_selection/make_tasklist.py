import glob
import argparse
import sys, os, glob

def parse_args():
    parser = argparse.ArgumentParser(description='Process Reddit parquet file(s).')
    parser.add_argument('-i', '--input',type=str)
    args = parser.parse_args()
    return(args)

if __name__ == "__main__":
    args = parse_args()

    if args.input == 'processing_comments_per_subreddit':
        parquet_files = glob.glob('/gscratch/comdata/output/reddit_comments_by_subreddit.parquet/*.parquet')
        outfile_name = 'tasklist_rcomments'
    elif args.input == 'processing_submissions_per_subreddit':
        parquet_files = glob.glob('/gscratch/comdata/output/reddit_submissions_by_subreddit.parquet/*.parquet')
        outfile_name = 'tasklist_rsubmissions'
    else:
        print("Error in dir choice: Should be <processing_comments_by_subreddit> or <processing_submissions_per_subreddit>\nGot{}.".format(args.input))
        sys.exit()
    print('{} files of this type'.format(len(parquet_files)))
    print(parquet_files[0])

    with open(outfile_name, 'w') as f:
        for p in parquet_files:
            line = 'python3 parquet_parse_one.py -i {} -o {}\n'.format(p, args.input)
            f.write(line)

    print('Done.')
