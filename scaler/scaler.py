import sys
import os
sys.path.append(os.path.abspath('.'))

from flask import Flask #import the Flask tool for writing web servers
from functools import wraps #a decoration tool
from flask import request, Response, jsonify #import some convinience functions from flask.
import logging #logging
import os,json #OS functions
from cloudfoundry import CloudFoundryInterface #The CF interface written by Matt Cowger
import redis
import boto  # the library for interacting with AWS services
from config import Config #Easy config files

redis_rq_creds = {}
redis_images_creds = {}
s3_creds = {}
twitter_creds = {}
configstuff = {}

logger = logging.getLogger()  # Grab the logging instance for our app, so we can make changes
logger.setLevel(logging.DEBUG)  # LOG ALL THE THINGS!

formatter = logging.Formatter("%(asctime)s [%(module)s:%(funcName)s] [%(levelname)s] %(message)s")
# and make them look prettier

ch = logging.StreamHandler()  #set up a logging handler for the screen
ch.setLevel(logging.DEBUG)  #make it only spit out INFO messages
ch.setFormatter(formatter)  #make it use the pretty format
logger.addHandler(ch)  #and finally add it to the logging instance


logging.getLogger("requests.packages.urllib3.connectionpool").setLevel(logging.WARN)
logging.getLogger("oauthlib").setLevel(logging.WARN)
logging.getLogger("requests_oauthlib").setLevel(logging.WARN)


if "VCAP_SERVICES" in os.environ:
    rediscloud = json.loads(os.environ['VCAP_SERVICES'])['rediscloud']
    for creds in rediscloud:
        if creds['name'] == "vascodagama-db":
            redis_rq_creds = creds['credentials']
        elif creds['name'] == "vascodagama-images":
            redis_images_creds = creds['credentials']
    userservices = json.loads(os.environ['VCAP_SERVICES'])['user-provided']
    for configs in userservices:
        if configs['name'] == "s3_storage":
            s3_creds = configs['credentials']
        elif configs['name'] == "configstuff":
            configstuff = configs['credentials']
else:
    cfg = Config(file('private_config_new.cfg'))
    redis_images_creds = cfg.redis_images_creds
    redis_rq_creds = cfg.redis_rq_creds
    s3_creds = cfg.s3_creds
    twitter_creds = cfg.twitter_creds
    configstuff = cfg.configstuff

logging.basicConfig(level=logging.DEBUG) #setup basic debugging.

redis_images = redis.Redis(host=redis_images_creds['hostname'], db=0, password=redis_images_creds['password'],
                           port=int(redis_images_creds['port']))
redis_queue = redis.Redis(
    host=redis_rq_creds['hostname'],
    db=0,
    password=redis_rq_creds['password'],
    port=int(redis_rq_creds['port'])
)

def clear_app():
    hashtag = redis_queue.get("hashtag")
    logging.info("Got request to reset. Will clear the db and bucket")
    logger.debug("flushing redis image db")
    redis_images.flushdb()
    logger.debug("flushing redis queue db")
    redis_queue.flushdb()
    logger.debug("opening s3 connection")
    logger.debug("repopulating the hashtag")
    redis_queue.set("hashtag",hashtag)
    s3conn = boto.connect_s3(s3_creds['access_key'], s3_creds['secret_key'], host=s3_creds['url'])  #set up an S3 style connections
    logger.debug("Getting bucket")
    bucket = s3conn.get_bucket(s3_creds['bucket_name'])  #reference to the S3 bucket.
    logger.debug("deleting bucket contents")
    for x in bucket.list():
        logger.info("Deleted image {} from object store".format(x.key))
        bucket.delete_key(x.key)
    return


def check_auth(username, password):
    """This function is called to check if a username /
    password combination is valid.  It only accepts 1 set of values :). TODO.
    """
    return username == configstuff['cf_user'] and password == configstuff['cf_pass']



def authenticate():
    """Sends a 401 response that enables basic auth"""
    return Response(
        'Could not verify your access level for that URL.\n'
        'You have to login with proper credentials', 401,
        {'WWW-Authenticate': 'Basic realm="Login Required"'})
        #A decoration function to require HTTP Basic Auth


#A decorator to require HTTP Basic auto for anything it decorates.
def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return authenticate()
        return f(*args, **kwargs)
    return decorated


#setup Flask
app = Flask(__name__)

#This will respond to requests for / and requires Auth.  If it gets run properly, it will return json of success.
@app.route('/')
@requires_auth
def index():
    logging.info("Got request for login test")
    return jsonify({'status': 'success'})


#this will respond to requests for /apps and requires auth.
@app.route('/apps')
@requires_auth
def apps():
    logging.info("Got request for apps")
    cfi = CloudFoundryInterface('https://api.run.pivotal.io', username=configstuff['cf_user'], password=configstuff['cf_pass']) #connect to CF
    cfi.login() #login

    app_list = [cf_app.name for cf_app in cfi.apps.itervalues()] #get the list of apps (this technique is called a list comprehension, BTW)
    applications = {'applications': sorted(app_list)} #sort it to be nice.
    return jsonify(applications) #send a JSON list of applications.


#This will response to requesty for /scale/<an app name>/<scale target>
@app.route('/scale/<appname>/<int:target>')
@requires_auth
def scale_app(appname, target):
    logging.info("Got request to scale app \'{}\' to {}".format(appname, target))
    cfi = CloudFoundryInterface('https://api.run.pivotal.io', username=configstuff['cf_user'], password=configstuff['cf_pass']) #again, connect to CF
    cfi.login() #and login

    app_list = [cf_app.name for cf_app in cfi.apps.itervalues()] # make sure the app we got was in the list of apps.
    if appname not in app_list:
        return jsonify({'status': 'failure'})
    else:
        cfi.scale_app(cfi.get_app_by_name(appname),target) #and if it is, scale it as requested.
        return jsonify({"status":"success"})


@app.route('/newhashtag/<hashtag>')
@requires_auth
def change_hashtag(hashtag):
    logger.info("Changing hashtag to {}".format(hashtag))
    redis_queue.set("hashtag",hashtag)
    clear_app()
    return jsonify({"status":"success"})

@app.route('/reset')
@requires_auth
def reset_app():
    clear_app()
    return jsonify({"status":"success"})

def run():
    app.run(host='0.0.0.0', port=int(os.getenv('VCAP_APP_PORT', '5000')))

if __name__ == "__main__":
    run()