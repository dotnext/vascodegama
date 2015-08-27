import sys
import os
sys.path.append(os.path.abspath('.'))

from flask import Flask #import the Flask tool for writing web servers
from functools import wraps #a decoration tool
from flask import request, Response, jsonify #import some convinience functions from flask.
import logging, logging.config #logging
import os,json #OS functions
from cloudfoundry import CloudFoundryInterface #The CF interface written by Matt Cowger
import redis
import utils
import boto  # the library for interacting with AWS services
from config import Config #Easy config files

logging.config.dictConfig(utils.get_log_dict())
logger = logging.getLogger('vascodagama.scaler')



redis_images = utils.get_images_redis_conn()
redis_queue = utils.get_rq_redis_conn()
q = utils.get_rq()


def batch_gen(data, batch_size):
    for i in range(0, len(data), batch_size):
            yield data[i:i+batch_size]

def clear_app():
    s3_creds = utils.s3_creds()
    hashtag = redis_queue.get("hashtag")
    logger.info("Got request to reset. Will clear the db and bucket")
    logger.debug("flushing redis image db")
    redis_images.flushdb()
    logger.debug("flushing redis queue db")
    redis_queue.flushdb()

    logger.debug("repopulating the hashtag")
    redis_queue.set("hashtag",hashtag)

    return 1


def check_auth(username, password):
    """This function is called to check if a username /
    password combination is valid.  It only accepts 1 set of values :). TODO.
    """
    configstuff = utils.configstuff()
    return username == "user" and password == "pass"



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
    logger.info("Got request for login test")
    return jsonify({'status': 'success'})


#this will respond to requests for /apps and requires auth.
@app.route('/apps')
@requires_auth
def apps():
    configstuff = utils.configstuff()
    logger.info("Got request for apps")
    cfi = CloudFoundryInterface('https://api.run.pivotal.io', username=configstuff['cf_user'], password=configstuff['cf_pass']) #connect to CF
    cfi.login() #login

    app_list = [cf_app.name for cf_app in cfi.apps.itervalues()] #get the list of apps (this technique is called a list comprehension, BTW)
    applications = {'applications': sorted(app_list)} #sort it to be nice.
    return jsonify(applications) #send a JSON list of applications.


#This will response to requesty for /scale/<an app name>/<scale target>
@app.route('/scale/<appname>/<int:target>')
@requires_auth
def scale_app(appname, target):
    configstuff = utils.configstuff()
    logger.info("Got request to scale app \'{}\' to {}".format(appname, target))
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
    deleted = clear_app()
    return jsonify({"status":"success", "count":deleted})

@app.route('/reset')
@requires_auth
def reset_app():
    deleted = clear_app()
    return jsonify({"status":"success", "count":deleted})

def run():
    app.run(host='0.0.0.0', port=int(os.getenv('VCAP_APP_PORT', '5000')))

if __name__ == "__main__":
    run()
