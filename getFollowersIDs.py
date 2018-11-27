from __future__ import print_function
import sys
import csv
import time
import ConfigParser
import tweepy
import json
import ssl
from functools import wraps
import argparse
from  more_itertools import unique_everseen

# Having SSL error while trying to connect to Twitter Streaming API,
# http://goo.gl/z4RQY1
# Python Requests requests.exceptions.SSLError: [Errno 8] _ssl.c:504:
# EOF occurred in violation of protocol
def sslwrap(func):
    @wraps(func)
    def bar(*args, **kw):
        kw['ssl_version'] = ssl.PROTOCOL_TLSv1
        return func(*args, **kw)
    return bar

def selectNewspapers(db, col, skip):
    print ("Opening " + db + " at column " + str(col) + " at row " + str(skip))
    nps = []
    with open(db,'rb') as fp:
        while skip:
            fp.readline()
            skip = skip - 1
        csvr = csv.reader(fp)
        for row in csvr:
            if row[col] != '':
                nps.append(row[col])
    return list(unique_everseen(nps))

def setupTwitterAPI():
    # load config file with the keys
    config = ConfigParser.RawConfigParser()
    config.read('./creds')

    if not config.has_section('twitter'):
        print ("creds file is missing the twitter section. Exiting.")
        sys.exit(2)

    print ("Loaded configuration file from local credentials.")

    # oauth twitter
    KEY = config.get('twitter', 'key')
    KEY_SECRET = config.get('twitter', 'key_secret')
    TOKEN = config.get('twitter', 'token')
    TOKEN_SECRET = config.get('twitter', 'token_secret')
	
    auth = tweepy.OAuthHandler(KEY, KEY_SECRET)
    auth.set_access_token(TOKEN, TOKEN_SECRET)

    api = tweepy.API(auth,
                     parser=tweepy.parsers.JSONParser(),
                     wait_on_rate_limit=True,
                     wait_on_rate_limit_notify=True)

    print ("Twitter OAuth authorization complete.")
    print ("Tweepy API version " + tweepy.__version__)
    return api


def downloadIDs(nps, row):
    ssl.wrap_socket = sslwrap(ssl.wrap_socket)
    api = setupTwitterAPI()
    llen = len(nps)
    for n in nps:
        print ("\rRetrieving followers for {0} ({1}/{2} at row {3})".format(n,str(nps.index(n)+1),str(llen),row))
        with  open("data/friends_"+n, "a+") as f:
            try:
                for page in tweepy.Cursor(api.followers_ids, id=n).pages():
                    for item in page['ids']:
                        f.write(str(item) + '\n')
            except tweepy.TweepError as e:
                print (e)
                continue
        row = row + 1

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Retrieve Twitter followers IDs.')

    parser.add_argument('db',
                   help='the csv file containing the list of twitter handles')
    parser.add_argument('-c', '--column', type=int, default=0,
                   help='the column in the csv file. Defaults to 0')
    parser.add_argument('-r', '--row', type=int, default=1,
                            help='the row to start at. Defaults to 1 to skip header.')

    args = parser.parse_args()

    nps = selectNewspapers(args.db, args.column, args.row)
    print ("Processing: " + str(len(nps)) + " unique Twitter accounts.")
    downloadIDs(nps, args.row)
    print ("Done. Finally...")
