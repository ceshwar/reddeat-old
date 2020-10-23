'''
Created on 25/apr/2016

@author: Mattia
'''


import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np
from praw.helpers import convert_id36_to_numeric_id, convert_numeric_id_to_id36
import seaborn as sns
# sns.set_palette("hls")
sns.set_style("whitegrid", {'axes.grid' : False})
import time
import praw
if __name__ == '__main__':
    fname = "ids.txt"
    df = pd.read_csv(fname, header = None, names = ["id", "created", "crawled"])
    df.dropna(inplace=True)
    df["created_utc"] = df.created.apply(lambda x: datetime.utcfromtimestamp(x))
    df["crawled_utc"] = df.crawled.apply(lambda x: datetime.utcfromtimestamp(x))
    print "crawl started:", df.crawled_utc.min()
    print "crawl ended:", df.crawled_utc.max()
    print "crawl lasted for:", df.crawled_utc.max()-df.crawled_utc.min()
    print "comment ids collected:", len(df)
    print "avg comment rate: %2.4f comments per second" % (len(df)/(df.crawled_utc.max()-df.crawled_utc.min()).total_seconds())
#     plt.show()
#     print df.id.apply(lambda x: convert_id36_to_numeric_id(str(x[3:])))
    df["num_ids"] = df.id.apply(lambda x: convert_id36_to_numeric_id(str(x[3:])))
    num_ids=df["num_ids"]
#     print df.describe()
    df.set_index("num_ids", inplace=True)
    df["crawl_lag"] = df.crawled - df.created
    ax=df.crawl_lag.plot(kind="area", zorder=300, alpha=.8, lw=0)
#     plt.savefig("crawl lag in seconds.pdf")
#     print "missing ids:", num_ids.max()-num_ids.min() - len(num_ids), "/", len(num_ids)
    missing_ids = pd.Series(sorted(set(range(num_ids.min(), num_ids.max()+1)) - set(num_ids)))#.apply(lambda x: u"t1_"+convert_numeric_id_to_id36(int(x)))
#     print df.index.min()
    for i in missing_ids:
        plt.axvline(i, color='k', alpha=.2, zorder=0)
#     pd.DataFrame(data = zip(missing_ids.values, np.ones_like(missing_ids.values))).plot(ls="^")
#     sns.despine()
    ax.grid(False)
    plt.show()
    
    USER_AGENT = "python:automod:v0.1 (by /u/hide_ous)"
    r = praw.Reddit(USER_AGENT)
    ids = list(missing_ids.apply(lambda x: u"t1_"+convert_numeric_id_to_id36(int(x))))
    times = []
    limit=100
    results=[]
    for i in range(0, len(ids)-limit+1, limit):
        start_time= time.time()
        results.extend(r.get_info(thing_id=ids[i:i+limit]))
        times.append(time.time()-start_time)
        print times[-1]
    print "executed in %2.2fs ([%2.2f +- %2.2f x %d])" %  (np.sum(times), np.mean(times), np.std(times), len(times))
    print "done fetching results"
#     for rr in results:
#         print rr.json_dict
    results = pd.DataFrame((rr.json_dict for rr in results))
    print results.describe()
    print "writing results"
    results.to_csv("missing_ids.csv", encoding='utf8')
    results = pd.read_csv(fname, encoding='utf8', index_col=0)
    print "[removed] or [deleted] comments: %d" % results.body.apply(lambda x: (x == '[removed]') or (x == '[deleted]')).sum()
    non_empty_results = results[results.body.apply(lambda x: not ((x == '[removed]') or (x == '[deleted]')))]
    print "remaining comments: %d" % len(non_empty_results)
    # check if the ones that are non-empy are still in the thread
    non_empty_comments_refetched = []
    for s in r.get_submissions(list(non_empty_results.link_id.unique())):
        print "expanding comments for submission", s
        s.replace_more_comments(limit=None, threshold=0)
        print "expanded comments"
        non_empty_comments_refetched.extend([c.json_dict for c in praw.helpers.flatten_tree(s.comments)])
    non_empty_comments_refetched = pd.DataFrame(non_empty_comments_refetched)
    n, o = (non_empty_comments_refetched.set_index("name", ), non_empty_results.set_index("name", ))
    m = o.join(n, lsuffix='_o', rsuffix='_n') 
    print "bye"