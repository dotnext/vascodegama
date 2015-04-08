from flask import Flask, render_template #import our web server
import logging #with loggins

import redis #and redis
from config import Config # and the config files
import os #and some OS functions
import json #json functions
import socket # i have no idea what this is
from logging.handlers import SysLogHandler #import syslog handler

class ContextFilter(logging.Filter):
  hostname = socket.gethostname()
  def filter(self, record):
    record.hostname = ContextFilter.hostname
    return True


redis_images_creds = {}

if "VCAP_SERVICES" in os.environ:
    rediscloud = json.loads(os.environ['VCAP_SERVICES'])['rediscloud']
    for creds in rediscloud:
        if creds['name'] == "vascodagama-db":
            redis_rq_creds = creds['credentials']
        elif creds['name'] == "vascodagama-images":
            redis_images_creds = creds['credentials']
    userservices = json.loads(os.environ['VCAP_SERVICES'])['user-provided']
    for configs in userservices:
        if configs['name'] == "configstuff":
            configstuff = configs['credentials']
else:
    cfg = Config(file('private_config_new.cfg'))
    redis_images_creds = cfg.redis_images_creds
    redis_rq_creds = cfg.redis_rq_creds
    s3_creds = cfg.s3_creds
    twitter_creds = cfg.twitter_creds
    configstuff = cfg.configstuff


logger = logging.getLogger('')
logger.setLevel(logging.DEBUG)
f = ContextFilter() #create context filter instance
logger.addFilter(f) #add the filter to the logger

formatter = logging.Formatter("%(asctime)s [%(module)s:%(funcName)s] twitter_photos [%(levelname)-5.5s] %(message)s")


# loggly_handler = loggly.handlers.HTTPSHandler(url="{}{}".format(credentials["Loggly"]["url"], "gui"))
# loggly_handler.setLevel(logging.DEBUG)
# logger.addHandler(loggly_handler)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(formatter)
logger.addHandler(ch) #and finally add it to the logging instance
logging.getLogger("requests.packages.urllib3.connectionpool").setLevel(logging.WARN)
#setup logging, check twitter_watch for details on what all the above does.


#setup flask
app = Flask(__name__)

#connect to redis
redis_images = redis.Redis(host=redis_images_creds['hostname'], db=0, password=redis_images_creds['password'],
                           port=int(redis_images_creds['port']))
#Setup a connection that will be used by RQ (each redis connection instance only talks to 1 DB)
redis_queue = redis.Redis(
    host=redis_rq_creds['hostname'],
    db=0,
    password=redis_rq_creds['password'],
    port=int(redis_rq_creds['port'])
)

#gets a list of random URLS from refis.
def get_random_urls(count=100):
    pipe_keys = redis_images.pipeline() #setup 2 batches
    pipe_urls = redis_images.pipeline()
    keys = []
    for i in range(0,count): # get 'count' random keys
        pipe_keys.randomkey()

    for key in pipe_keys.execute(): #for each one of those random keys
        pipe_urls.hget(key,"url") #get the URL property.

    urls = pipe_urls.execute() #the list of URLs is the result.
    return urls #return it.


#This responds to requests for "/"
@app.route('/')
def dashboard():
    urls = get_random_urls() # get the list of URLs
    hashtag = str(redis_queue.get("hashtag"))
    return render_template('default-us.html',urls=urls,hashtag=hashtag) #Responsed by feeding that list of URLs into the template, and returning the rendered HTML

if __name__ == "__main__":
    port = int(os.getenv('VCAP_APP_PORT', '5000')) #liston in VCAP_APP_PORT if known, otherwise 5000
    logging.info("Running on port {}".format(port))
    app.run(host='0.0.0.0', port=port) #listen on specific port and all IP addresses.
