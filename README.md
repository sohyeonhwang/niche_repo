# online niche communities project
This is a repo to keep track of some files for the niche communities project.

* The `data` and `subreddit_selection` folders have various data files that were used to figure out a set of subreddits to recruit from. Most of the stuff in `data` is a repeat of `subreddit_selection`. 

## not here but on Hyak
Lots of data files that might be related to processing but too big to put on repo are on Hyak. Can be accessed: `/usr/lusers/sohw/niche`. For example:

* `2020-11-06-encoded.csv`, which is from `https://frontpagemetrics.com/list-all-subreddits, is there

## directories
In `data`:

* `20201106encoded_and_filtered_overlap.tsv` is the overlap between `2020-11-06-encoded.csv` and `filtered_small_subreddits.tsv`

In `subreddit_selection`, data processed on Hyak in late October/early November:

* `parquet_parse_one.py` processes the Reddit parquet files and outputs to `processing_comments_per_subreddit` and `processing_submissions_per_subreddit` in the `data` directory or in the case of subreddit keys, `subreddit_id_key` directory. `make_tasklist.py` is used to generate tasklists .(`tasklist_rcomments`,`tasklist_rsubmissions`) to run `parquet_parse_one.py`
* Use `check_missing_tasklist.py` to find the missing files that didn't process, and run those with the bash script: `missing_spark_run.sh`

* I think the results of this were unsatisfactory, and I redid all with Spark. Probably simply ran:

    ```
    python3 parquet_parse_spark.py -i /gscratch/comdata/output/reddit_comments_by_subreddit.parquet
    ```

    * I did comments as the count metric rather than submissions, so I did not run on `reddit_submissions_by_subreddit.parquet`. I think my logic was because of a file that Jeremy had shared was filtered by unique contributors on comments. More generally, there are usually more comments than posts. 

* `parquet_parse_spark.py` processes the Reddit parquet files. These output to the working directory (`key_out_comments.tsv` and `out_comments.tsv`).

    * Specifically, I group the comments by `"subreddit","Year","Month"`, counting the number of comments by comment id, and summing up the number of unique authors involved (and summing up the number of comments by deleted author, for which we no longer have unique identifiers)
    * Because we drop the subreddit's real name for now in processing, I also create a subreddit to subreddit key file, which is useful for the monthly processing step (next)

* `process_monthly_subreddit.py` to make `all_subreddits.tsv` and (currently commented out) `filtered_small_subreddits.tsv`. Note that `process_monthly_subreddit.py` filters using `out_comments.tsv` data.

    * `process_monthly_subreddit.py` takes in the output files in `out_comments.tsv` which is a directory, not actually a tsv file.
    * I pd.concat all the files in out_comments.tsv as dfs into one df, and sum up the counts for subreddit_id-year-month. This sums the number of comments, the number of unique contributors, and the number of deleted authors for each year-month combo for each subreddit. Then we also sum up the number of different years (`year_nunique`) and months (`s_month_count`, sum month count) the subreddit has been active (has at least 1 comment)
    * We then calculate the monthly average number of unique contributors for each subreddit.
    * This results in each row having subreddit_id, number of years, number of months, average unique contributors monthly. This metadata is outputted to `all_subreddits.tsv`.
    * We filter down to find small but active communities.

## re: filtering

We initially had a filtering that resulted in about 15k subreddits, where we filtered for size first, in terms of unique authors. The idea was to get communities that had at least some level of activity, though a relatively low but sustained level (number of unique commentors) per month, for at least 36 unique months. You could imagine that there are communities that are dead in between long periods (or in recent months) or had explosive growth recently, or so on. So the criteria is at least active enough:

    ```
        # filtering for the subreddit/year/month combos that are in the certain range of unique authors
        small_df = grouped_df.loc[ grouped_df.author_nunique <= 100]
        small_df = small_df.loc[ small_df.author_nunique >= 5 ]
        # and then length of activeness
        small_df = small_df.loc[ small_df.year_nunique >= 3 ]
        small_df = small_df.loc[ small_df.month_count >= 36 ]
        # and then calculated s_month_count (small month counts), which gives the number of months that fit the author_nunique <= 100 and small_df.author_nunique >= 5 . so s_month_count is always less than or equal to month_count
        # we then filtered those communities that had at least 36 months of the unique author count criteria imposed.
        small_df = small_df.loc[ small_df.s_month_count >= 36 ]
    ```

There is a simpler, alternative where we just filter number of contributors by average overall. But the first one is more precise in the sense of sustained levels of certain amount of activity. Here is the code to get the "average" (`filtered_avg_small_subreddits.tsv`):

    ```
        # relatively active for some time
        small_df = small_df.loc[ small_df.year_nunique >= 3 ]
        small_df = small_df.loc[ small_df.month_count >= 36 ] 

        # but small in number of contributors
        avg = small_df.groupby(by=["subreddit_id"]).agg({'author_nunique':'mean'}).reset_index().rename(columns={'author_nunique':'avg_auth_unique'})
        avg = avg.loc[ avg.avg_auth_unique <= 100]
        avg = avg.loc[ avg.avg_auth_unique >= 5]

        # this results in about 25k subreddits
    ```
