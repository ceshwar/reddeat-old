# Reddeat #

A file-based crawler for comments removed from Reddit, written in python.

### What is in the repository? ###
* reddeat.py is the main code for the crawler;

* get_manual_authorization.py is a stub script for obtaining the necessary access tokens for the application (only needed once);

* RemoteException.py is a helper module that helps displaying exceptions in multithreaded environments;

* praw.ini is a sample [PRAW](https://praw.readthedocs.io/en/stable/) configuration file, to be completed with te application information and respective access tokens;

* check_ids.py is a gist for exploring missing comment fullnames from a previous log file.

### How do I get set up? ###
All dependencies (notably praw, watchdog, numpy) are available through pip. 

Set up [OAuth2](https://praw.readthedocs.io/en/stable/pages/oauth.html). In brief:

1. Go to [Reddit](https://www.reddit.com/prefs/apps/) and create an app.

2. Take note of the client_id, client_secret, and redirect_uri fields.

3. Paste the fields above in the respective get_manual_authorization.py variables.

4. Run get_manual_authorization.py, and note down your refresh_token.

5. Fill in all missing information in the sample praw.ini file provided, and move it to the [appropriate location](https://praw.readthedocs.io/en/stable/pages/configuration_files.html#config-file-locations).

### How do I keep reddeat running? ###
If cron is available:

* crontab -e

* \*/5 \* \* \* \* pgrep -f reddeat.py || nohup python /path/to/reddeat.py