# -*- coding: utf-8 -*-
'''
Created on 22/apr/2016

This module 
1) collects all new comments posted on Reddit, and stores them
2) re-collects the comments after a period of time, and stores the updated version,
 if removed/deleted from Reddit.

Comments are collected using the PRAW Reddit API wrapper. Authentication is performed
through OAuth2 -- app credentials must be stored in the corresponding praw.ini file
for this script to work. A sample praw.ini file is provided with this software, as well 
as a script to obtain app authentication credentials from the user for the first time.

Operation 1) is performed in the reddeat routine. Comments are fetched through a PRAW
helper function, asking updates on the 'all' subreddit. Comments are stores as json
objects, one per line, using the standard python logging module. Empty fields are not 
stored to file for space optimization, however it is easy to get a list of all returned
json keys. The log file is periodically rotated, for resiliency and re-processing purposes. 
It appears that PRAW's helper function is missing some comment fullnames, which should 
be base36-encoded serial integers: manual inspection suggests those comments were 
automatically moderated, and never exposed to the public. While this script ignores 
these automatically moderated comments, Reddit will respond if asked the specific 
fullnames.

Operation 2) is carried on by the recheck_log_file function. When the current log file 
is rotated, a file system monitor calls this function on the just-closed log file. 
Comments are re-fetched by fullname through the API's info endpoint, in batches,
ensuring that the desired time passed between since the first comment in the batch
was originally posted. Pree.ch found out that on the slowest moderated subreddit in their
tests, moderators acted on average after 7 hours from the original posting time.
If a comment meets the criteria defined in check_comment_removed, the re-fetched version 
is stored using the same format as for operation 1), in a different log file. At the end 
of operation 2), both the original log file, and the log file for the removed comments,
are bzipped. 

@author: Mattia
'''

import logging
import os
#from future.backports.socket import errno
from logging.handlers import TimedRotatingFileHandler
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
from itertools import izip_longest
from shutil import copyfileobj
import bz2
from functools import partial
import collections
from datetime import datetime
import time
import praw
import urllib2
import requests
import json
from numbers import Number
import sys
import numpy as np
from optparse import OptionParser
import codecs
from socket import errno
import RemoteException
from itertools import islice

SECONDS = 1
MINUTES = 60*SECONDS
HOURS = 60*MINUTES
DAYS = 24*HOURS
RUN_FOR = None#1*HOURS # how long to run the crawler, in seconds. None or negative for no stop

LOGGER_NAME = "reddeat" # logger for comments
LOGGER_FOLDER = "log/" # where to store the comment logs
REMOVED_FILE_SUFFIX = ".removed" # suffix for the file containing the re-fetched comments that were removed
LOG_ROTATION_UNIT = "M" # as defined in watchdog
LOG_ROTATION_INTERVAL = 1 # as defined in watchdog
ERROR_LOGGER_NAME = LOGGER_NAME + "_error" # logger for execution errors
ERROR_LOGGER_FOLDER = "log/" # where to store the error logs

REDDIT_COMMENT_BATCH_SIZE = 100 # 100 is ok, just to play safe with API limits -- reddit's output is roughly 30 comments/s, APIs allow for 100 comments/s requests
DEFAULT_SLEEP_TIME = 1 * MINUTES # how long to sleep if errors happen
USER_AGENT = "automod v0.1 by /u/hide_ous" # user agent for the application
#r = praw.Reddit(USER_AGENT)
r = praw.Reddit('reddit', user_agent='python:automod:v0.1 (by /u/hide_ous)')
print r.user.me() # chech that authorization succeeds
r.read_only=True
    
def to_json(praw_entity):
    '''
    Remove items that have a null/empty value
    :param praw_entity: dict

    :returns: json-encoded representation or praw_entity
    '''
    to_dump = {i:j for i, j in praw_entity.iteritems() if not i.startswith('_')}
    if ('author' in to_dump) and to_dump['author'] and hasattr(to_dump['author'], 'name'):
        to_dump['author'] = to_dump['author'].name
    if ('subreddit' in to_dump) and to_dump['subreddit'] and hasattr(to_dump['subreddit'], 'display_name'):
        to_dump['subreddit'] = to_dump['subreddit'].display_name
    return json.dumps(to_dump, check_circular=False)

def _check_not_null(x):
    '''
    Check if an item is not null. Non-nan numbers and booleans are considered not-null
    :param x: the item to check
    '''
    if isinstance(x, (Number, bool)):
        return not np.isnan(x)
    else:
        return x
    
def strip_empty_fields(d):
    '''
    Remove items that have a null/empty value
    :param d: dict

    :returns: d if d is dict, or a copy of d with empty-valued items removed
    '''
    if type(d) is dict:
        return dict((k, strip_empty_fields(v)) for k, v in d.iteritems() if _check_not_null(v) and strip_empty_fields(v))
    else:
        return d    
    
def mkdir_p(path):
    '''
    Simulate mkdir -p in linux environments: create a directory if it does 
    not exist, creating intermediate directories if necessary
    :param path: path for the new directory
    '''
    try:
        os.makedirs(path)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else: raise

def check_comment_removed(old_comment_, new_comment_):
    '''
    Checks if a comment has been removed from reddit, given an original and a later version 
    :param old_comment_: the original version of the comment (dict)
    :param new_comment_: the re-fetched version of the comment (PRAW Comment instance)
    
    :returns: True, if:
        - the author in the new version is None, or it contains [deleted] or [removed]
        - the body in the new version is None, or it contains [deleted] or [removed]
        - the old or the new version of the comment contain non-empty/zero/null values for fields:
            banned_by, mod_reports, user_reports, num_reports, removal_reason, report_reason
    '''
    
    old_comment, new_comment = collections.defaultdict(lambda: None), collections.defaultdict(lambda: None)
    old_comment.update(old_comment_)
    new_comment.update(new_comment_.__dict__)
#    for k in new_comment:
#        if new_comment[k] == 'None': 
#            new_comment[k] = None
    
    if (not new_comment["author"]) or (not hasattr(new_comment["author"], 'name')) or ("[deleted]" in new_comment["author"].name) or ("[removed]" in new_comment["author"].name): return True 
    if (not new_comment["body"]) or ("[deleted]" in new_comment["body"]) or ("[removed]" in new_comment["body"]): return True   
    if old_comment["banned_by"] or new_comment["banned_by"]: return True
    if old_comment["mod_reports"] or new_comment["mod_reports"]: return True
    if old_comment["user_reports"] or new_comment["user_reports"]: return True
    if old_comment["num_reports"] or new_comment["num_reports"]: return True
    if old_comment["removal_reason"] or new_comment["removal_reason"]: return True
    if old_comment["report_reason"] or new_comment["report_reason"]: return True
    return False

@RemoteException.showError
def recheck_log_file(dest_fpath, removed_fsuffix, comment_batch_size = 100, delay = 1*DAYS):
    '''
    Given a log file of comments, re-fetch them from reddit by comment fullname and, if deleted,
    store the re-fetched version to file. Then, bzip both the original file, and the file
    containing the removed comments.
    
    :param dest_fpath: comment log file. comments are json entries, one per line
    :param removed_fsuffix: removed comment log file suffix, appended to dest_fpath
    :param comment_batch_size: how many comment fullnames to fetch per Reddit API call
    :param delay: how many seconds should pass between the original comment's post time, and the refetch time 
    '''
    error_logger = logging.getLogger(ERROR_LOGGER_NAME)
    removed_fpath = dest_fpath+removed_fsuffix
    try:
        with codecs.open(dest_fpath, 'r', encoding='utf8') as log_f:
            with codecs.open(removed_fpath, "w+", encoding='utf8') as f:
                # get comment_batch_size comments from the original log file
#                for next_n_lines in izip_longest(*[log_f] * comment_batch_size):
                next_n_lines = list(islice(log_f, comment_batch_size))
                while next_n_lines :
                    original_comments = [json.loads(s, encoding="utf8") for s in next_n_lines if s]
                    n_original_comments = len(original_comments)
                    original_comments = {s["name"]: s for s in original_comments}
                    comment_ids = list(original_comments.keys())
                    min_created_time_utc = np.min([np.float(s["created_utc"]) for s in original_comments.values()])
                    # wait for delay to occur before the first comment in the batch and the current time
                    needs_to_wait = delay + int((datetime.utcfromtimestamp(min_created_time_utc) - datetime.utcnow()).total_seconds())
                    if needs_to_wait > 0:
                        error_logger.debug("wait before refetching: sleeping %d seconds" %(needs_to_wait,))
                        time.sleep(needs_to_wait)
                    try:
                        # re-fetch them from reddit
                        refetched_comments = r.info(fullnames=comment_ids) or []
                        refetched_comments = {s.name: s for s in refetched_comments}
                        removed_comments = []
                        for c in original_comments:
                            if c not in refetched_comments:
                                # if the comment was not in reddit's response, add its fullname to the list
                                removed_comments.append((original_comments[c], {"name":c}))
                            elif check_comment_removed(original_comments[c], refetched_comments[c]):
#                            elif check_comment_removed(original_comments[c], refetched_comments[c]):
                                # if the comment has been removed/deleted, add it to the list
                                removed_comments.append((original_comments[c], refetched_comments[c]))  
                        for _, refetched_comment in removed_comments:
                            # write removed/deleted comments to file
                            
                            f.write(to_json(strip_empty_fields(refetched_comment.__dict__))+'\n')
#                            f.write(json.dumps(strip_empty_fields(refetched_comment), check_circular=False)+'\n')
                        error_logger.debug("found %d/%d removed comments" % (len(removed_comments),n_original_comments))
                    except urllib2.HTTPError, e:
                        error_logger.error("Reddit is down (error %s), sleeping, and dropping refetched comments" % e.code)
                        error_logger.critical(str(e))
                        time.sleep(DEFAULT_SLEEP_TIME)
                    except requests.exceptions.RequestException, e:
                        error_logger.error("connection to Reddit is acting up. sleeping, and dropping refetched comments")
                        error_logger.error(str(e))
                        time.sleep(DEFAULT_SLEEP_TIME)
                    except Exception, e:
                        error_logger.critical("couldn't Reddit: %s. sleeping, and dropping refetched comments" % (str(e),))
                        time.sleep(DEFAULT_SLEEP_TIME)
                    except:
                        error_logger.critical("unexpected error: %s. sleeping, and dropping refetched comments" % (str(sys.exc_info()),))
                        time.sleep(DEFAULT_SLEEP_TIME)

                    next_n_lines = list(islice(log_f, comment_batch_size))
                                                
        error_logger.debug("comment re-fetch done")
        
        # clean up
        error_logger.debug("compressing and archiving logs")
        for fpath in [dest_fpath, removed_fpath]:
            # compress the original file once done
            with open(fpath, 'rb') as infile:
                with bz2.BZ2File(fpath+'.bz2', 'wb', compresslevel=9) as outfile:
                    copyfileobj(infile, outfile)
            # remove the original file
            os.remove(fpath)
    except IOError:  
        error_logger.critical("File error occurred: %s" % (str(e),))
        
class LogCompletedEventHandler(PatternMatchingEventHandler):
    '''
    Calls a function when a file is moved
    '''
    def __init__(self, logger_fname, callback_func):
        '''
        :param logger_fname: name of the file to watch  
        :param callback_func: function to call when the file is moved. The function should 
                accept the moved file path as the first parameter
        '''
        self.logger_fname = logger_fname
        self.callback_func = callback_func
        PatternMatchingEventHandler.__init__(self, "*"+logger_fname, ignore_directories=True)
    def on_moved(self, event):
        error_logger = logging.getLogger(ERROR_LOGGER_NAME)
        error_logger.debug("log file rotated: %s" % ( str(event),))        
        self.callback_func(event.dest_path)       
     
def parse_command_line():   
    '''
    Parse command line arguments, and update global variables accordingly
    '''
    
    global LOGGER_NAME, LOGGER_FOLDER, REMOVED_FILE_SUFFIX, ERROR_LOGGER_NAME, \
        ERROR_LOGGER_FOLDER, RUN_FOR, LOG_ROTATION_UNIT, LOG_ROTATION_INTERVAL, \
        DEFAULT_SLEEP_TIME, REDDIT_COMMENT_BATCH_SIZE

    parser = OptionParser()
    parser.add_option("-n", "--log_name", action="store", type="string", dest="LOGGER_NAME", default=LOGGER_NAME, help="file name where to store comments")
    parser.add_option("-f", "--log_folder", action="store", type="string", dest="LOGGER_FOLDER", default=LOGGER_FOLDER, help="folder where to store comments. should terminate in /")
    parser.add_option("-r", "--removed_file_suffix", action="store", type="string", dest="REMOVED_FILE_SUFFIX", default=REMOVED_FILE_SUFFIX, help="suffix for the file containing the re-fetched comments that were removed")
    parser.add_option("-e", "--error_log_name", action="store", type="string", dest="ERROR_LOGGER_NAME", default=ERROR_LOGGER_NAME, help="file name where to log execution errors")
    parser.add_option("-l", "--error_log_folder", action="store", type="string", dest="ERROR_LOGGER_FOLDER", default=ERROR_LOGGER_FOLDER, help="folder where to log execution errors. should terminate in /")
    parser.add_option("-d", "--duration", action="store", type="int", dest="RUN_FOR", default=RUN_FOR, help="how long to run the crawler, in seconds. Zero or negative for no stop")
    parser.add_option("-u", "--rotation_unit", action="store", type="string", dest="LOG_ROTATION_UNIT", default=LOG_ROTATION_UNIT, help="S - Seconds; M - Minutes; H - Hours; D - Days; midnight - roll over at midnight; W{0-6} - roll over on a certain day (0 = Monday)")
    parser.add_option("-i", "--rotation_interval", action="store", type="int", dest="LOG_ROTATION_INTERVAL", default=LOG_ROTATION_INTERVAL, help="rotate log file this many LOG_ROTATION_UNITs")
    parser.add_option("-s", "--sleep", action="store", type="int", dest="DEFAULT_SLEEP_TIME", default=DEFAULT_SLEEP_TIME, help="how long to sleep if errors happen")
    parser.add_option("-b", "--batch_size", action="store", type="int", dest="REDDIT_COMMENT_BATCH_SIZE", default=REDDIT_COMMENT_BATCH_SIZE, help="how many (potentially removed) comment fullnames to ask Reddit at a time. should be <=100 to comply with API limits")
    (options, _) = parser.parse_args()
    # update global variables
    LOGGER_NAME = options.LOGGER_NAME
    LOGGER_FOLDER = options.LOGGER_FOLDER
    REMOVED_FILE_SUFFIX = options.REMOVED_FILE_SUFFIX
    ERROR_LOGGER_NAME = options.ERROR_LOGGER_NAME
    ERROR_LOGGER_FOLDER = options.ERROR_LOGGER_FOLDER
    RUN_FOR = options.RUN_FOR
    LOG_ROTATION_UNIT = options.LOG_ROTATION_UNIT
    LOG_ROTATION_INTERVAL = options.LOG_ROTATION_INTERVAL
    DEFAULT_SLEEP_TIME = options.DEFAULT_SLEEP_TIME
    REDDIT_COMMENT_BATCH_SIZE = options.REDDIT_COMMENT_BATCH_SIZE

def setup_error_logger():
    '''
    Setup logger for execution information
    '''
    error_logger_path = os.path.abspath(ERROR_LOGGER_FOLDER+ERROR_LOGGER_NAME)
    error_logger_dir = os.path.dirname(error_logger_path)
    mkdir_p(error_logger_dir)
    # log errors to file
    error_logger = logging.getLogger(ERROR_LOGGER_NAME)
    error_handler = logging.FileHandler(error_logger_path, delay=True)
    error_handler.setFormatter(logging.Formatter("%(asctime)s %(message)s", "%y/%m/%d %H:%M"))
    error_handler.setLevel(logging.INFO)
    error_logger.addHandler(error_handler)
    # log debug information to console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    error_logger.addHandler(console_handler)
    error_logger.setLevel(logging.DEBUG) 

def setup_comment_logger():
    '''
    Setup logger for comments fetched from Reddit, and start the file system monitor
    for triggering comment re-fetch when the logger gets rotated
    '''
    logger_path = os.path.abspath(LOGGER_FOLDER+LOGGER_NAME) 
    logger_dir = os.path.dirname(logger_path)
    logger_fname = os.path.basename(logger_path)
    mkdir_p(logger_dir)
    logger = logging.getLogger(LOGGER_NAME)
    # log comments to file, rotating files at a given rate
    handler = TimedRotatingFileHandler(logger_path, when=LOG_ROTATION_UNIT, interval=LOG_ROTATION_INTERVAL, backupCount=0, encoding="utf8", delay=False, utc=True)
    handler.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    
    # setup logger watchdog
    event_handler = LogCompletedEventHandler(logger_fname, partial(recheck_log_file, removed_fsuffix=REMOVED_FILE_SUFFIX, comment_batch_size=REDDIT_COMMENT_BATCH_SIZE, delay=handler.interval))
    observer = Observer()
    observer.schedule(event_handler, logger_dir, recursive=False)
    observer.setDaemon(True)
    observer.start()

def reddeat():
    '''
    Main routine: fetch new comments from Reddit, and log them to file.
    '''
    
    # get loggers
    error_logger = logging.getLogger(ERROR_LOGGER_NAME)
    logger = logging.getLogger(LOGGER_NAME)

    # define stopping conditions
    start_time = time.time()
    end_time = RUN_FOR and (RUN_FOR+start_time) or RUN_FOR
    done = False
    def _done():
        if done:
            return True
        elif end_time > 0:
            return time.time() > end_time
        else:
            return False

    # eat Reddit
    counter = 0
    error_logger.debug(USER_AGENT)
    error_logger.debug("%s - starting crawler" % (time.strftime("%y/%m/%d %H:%M"),))
    while not _done():
        try:
            #for comm in praw.helpers.comment_stream(r, subreddit="all", limit=None, verbosity=2):
            for comm in r.subreddit('all').stream.comments():
                try:
                    logger.info(to_json(strip_empty_fields(comm.__dict__)))
                except Exception, e:
                    
                    comment_name = ""
                    if comm and ("name" in comm.__dict__):
                        comment_name = comm.__dict__["name"]
                    error_logger.error(str(e))
                    error_logger.error("cannot persist comment %s" % comment_name)
                counter+=1
                if not (counter % 10**3):
                    error_logger.debug("%s - %d comments fetched" % (time.strftime("%y/%m/%d %H:%M"), counter))
                if _done():
                    break
        except urllib2.HTTPError, e:
            error_logger.error("Reddit is down (error %s), sleeping..." % e.code)
            error_logger.error(str(e))
            time.sleep(DEFAULT_SLEEP_TIME)
        except requests.exceptions.RequestException, e:
            error_logger.error("connection to Reddit is acting up. sleeping...")
            error_logger.error(str(e))
            time.sleep(DEFAULT_SLEEP_TIME)
        except (KeyboardInterrupt, SystemExit):
            error_logger.critical("caught user/system interrupt")
            done = True
            break
        except Exception, e:
            error_logger.critical("couldn't Reddit: %s" % (str(e),))
            error_logger.error(str(e))
            time.sleep(DEFAULT_SLEEP_TIME)
        except:
            error_logger.critical("an unknown error happened. sleeping")
            time.sleep(DEFAULT_SLEEP_TIME)
            
    error_logger.debug("%s - %d comments fetched. bye" % (time.strftime("%y/%m/%d %H:%M"), counter))

if __name__ == '__main__':
    
    # parse options from command line
    parse_command_line()

    # setup error logger
    setup_error_logger()
    
    # setup comment logger
    setup_comment_logger()

    # run
    reddeat()