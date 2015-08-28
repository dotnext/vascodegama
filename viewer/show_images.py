import sys
import os
sys.path.append(os.path.abspath('.'))

from flask import Flask, render_template #import our web server
import logging, logging.config, json #with loggins
import utils
import redis #and redis
from config import Config # and the config files
import os #and some OS functions
import json #json functions

logging.config.dictConfig(utils.get_log_dict())
worker_logger = logging.getLogger("vascodagama.worker")
watcher_logger = logging.getLogger("vascodagama.watcher")

logger = logging.getLogger('vascodagama.images')

from pyloggly import LogglyHandler


handler = LogglyHandler('d6985ec5-ebdc-4f2e-bab0-5163b1fc8f19', 'logs-01.loggly.com', 'images')
logger.addHandler(handler)


#setup flask
app = Flask(__name__)

#connect to redis
redis_images = utils.get_images_redis_conn()

#Setup a connection that will be used by RQ (each redis connection instance only talks to 1 DB)
redis_queue = utils.get_rq_redis_conn()

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
    return list(set(urls)) #return it.


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
