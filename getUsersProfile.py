#!/home/leo/anaconda2/bin/python
import sys
import csv
import time
#import ConfigParser
import tweepy
import dateutil.parser
import json
import ssl
from functools import wraps
import argparse
from  more_itertools import unique_everseen

reload(sys)
sys.setdefaultencoding('utf8')

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

def setupTwitterAPI():
    # load config file with the keys
    config = ConfigParser.RawConfigParser()
    config.read('./creds')

    if not config.has_section('twitter'):
        print "creds file is missing the twitter section. Exiting."
        sys.exit(2)

    print "Loaded configuration file from local credentials."

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

    print "Twitter OAuth authorization complete."
    print "Tweepy API version " + tweepy.__version__
    return api

def processIDs(db, skip, header, col):
    ssl.wrap_socket = sslwrap(ssl.wrap_socket)
    api = setupTwitterAPI()

    print "Opening ID file " + db + " at row " + str(skip) + ". Using column " + str(col)

    ocsv = open(db + ".csv", 'a')
    ojson = open(db + ".json", 'a')

    print "Writing to:"
    print "\t" + ojson.name
    print "\t" + ocsv.name

    info = "# Generated on " + time.strftime("%c") + "\n"
    hdr = "id;screen_name;name;created_at;followers_count;friends_count;lang;location;profile_image_url;statuses_count;url;latest_action;description"

    if not header:
        ocsv.write(info + hdr + "\n")
        ojson.write("[")

    chunk = []
    i = total = 0
    MAX = 100

    with open(db,'rb') as fp:
        while skip:
            fp.readline()
            skip = skip - 1
        csvr = csv.reader(fp)
        for row in csvr:
            chunk.append(row[col])
            i += 1
            total += 1
            if i == MAX-1:
                downloadProfiles(chunk, ocsv, ojson, api, total)
                chunk = []
                i = 0
        if chunk:
            total += len(chunk)
            downloadProfiles(chunk, ocsv, ojson, api, total)

    ocsv.close()
    if not header:
        ojson.write("\b]")
    ojson.close()

# js is json file, df is csv file
def downloadProfiles(ids, df, jf, api, total):
    limit_status = api.rate_limit_status()
    print "[" + time.strftime("%Y %m %d-%H:%M:%S") + "] Processing rows starting at " + str(total) + " with " +  str(limit_status["resources"]["application"]["/application/rate_limit_status"]["remaining"]) + " API calls remaining."
    pid = ''
    try:
        for user in api.lookup_users(ids):
            jf.write(json.dumps(user) + ",\n")
            t = ';'
            pid = str(user['id'])
            if user.has_key('status'):
                latest_action = str(dateutil.parser.parse(user['status']['created_at']))
            else:
                latest_action = ""
            seq = (pid,
                       user['screen_name'],
                       "\"" + user['name'] + "\"" ,
                       user['created_at'],
                       str(user['followers_count']),
                       str(user['friends_count']),
                       user['lang'],
                       "\"" + user['location'] + "\"",
                       user['profile_image_url'],
                       str(user['statuses_count']),
                       str(user['url']),
                       str(latest_action),
                       "\"" + user['description'].replace('\n', ' ') + "\"")
            df.write(t.join(seq) + '\n')

    except tweepy.TweepError as e:
        print "Error processing id " + pid,
        print "at "+ time.strftime("%Y%m%d %H%M%S")
        print e
        exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Retrieve Twitter followers\' complete profiles.')

    parser.add_argument('db',
                            help='a file with a twitter ID per line.')

    parser.add_argument('-r', '--row', type=int, default=0,
                            help='the row to start at. Defaults to 0.')

    parser.add_argument('-nh', '--no-header', action='store_true',
                            help='do not write header info to file.')

    parser.add_argument('-c', '--column', type=int, default=0,
                            help='the column in the csv file. Defaults to 0')

    args = parser.parse_args()

    processIDs(args.db, args.row, args.no_header, args.column)
    print "\nDone. Finally..."
