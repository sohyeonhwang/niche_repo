import pandas as pd
import numpy as np
import random

subreddits = pd.read_csv('./filtered_small_subreddits.tsv', sep='\t', header=0)
print(subreddits)
