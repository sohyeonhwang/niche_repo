import glob, os

print("Comments")
infiles = glob.glob("/gscratch/comdata/output/reddit_comments_by_subreddit.parquet/*.parquet")
infiles = [x[62:] for x in infiles]
outfiles = glob.glob("./data/processing_comments_per_subreddit/*")
outfiles = [x[45:-4] for x in outfiles]
#print(outfiles[0])

in_set = set(infiles)
out_set = set(outfiles)
print("In:{} \t Out:{}".format(len(in_set),len(out_set)))
print(in_set.symmetric_difference(out_set))

print("\nSubmissions")
infiles = glob.glob("/gscratch/comdata/output/reddit_submissions_by_subreddit.parquet/*.parquet")
infiles = [x[65:] for x in infiles]
outfiles = glob.glob("./data/processing_submissions_per_subreddit/*")
outfiles = [x[48:-4] for x in outfiles]
#print(outfiles[0])

in_set = set(infiles)
out_set = set(outfiles)
print("In:{} \t Out:{}".format(len(in_set),len(out_set)))
print(in_set.symmetric_difference(out_set))

