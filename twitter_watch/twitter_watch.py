from __future__ import print_function  # force the use of print(X) rather than print X
from config import Config  # import a module that makes config files easier
from TwitterAPI import TwitterAPI  # The twitter API
from PIL import ImageFilter, Image  # The Python Imaging Library
from StringIO import StringIO  # StringIO lets you treat a string in memory like a file handle
import requests  # request is a great library for interacting with web services
from pprint import pprint, pformat  # pprint can pretty print complex data structures
import logging  # the standard python logging library
import boto  # the library for interacting with AWS services
import socket  # i have no idea what this is
from boto.s3.key import Key  # Class to represent a S3 key
from boto.s3.lifecycle import Lifecycle, Expiration  # classes so we can set lifecycles/expirations on objects
import time  # basic time handling library
import redis  # redis library
import uuid  # library for creating unique IDs
import random  # lirbary for random numbers
from rq import Queue  # RQ, the job queueing system we use
from rq.decorators import job  # And the function decoration for it.
import dweepy  # the library we use for sending status updates.  Check http://dweet.io
import os, json




redis_rq_creds = {}
redis_images_creds = {}
s3_creds = {}
twitter_creds = {}
configstuff = {}

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
        elif configs['name'] == "twitter":
            twitter_creds = configs['credentials']
        elif configs['name'] == "configstuff":
            configstuff = configs['credentials']
else:
    cfg = Config(file('private_config_new.cfg'))
    redis_images_creds = cfg.redis_images_creds
    redis_rq_creds = cfg.redis_rq_creds
    s3_creds = cfg.s3_creds
    twitter_creds = cfg.twitter_creds
    configstuff = cfg.configstuff



logger = logging.getLogger(__name__)  # Grab the logging instance for our app, so we can make changes
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

#From this particular library, supress certain messages.

logger.debug("Connecting to Redis for images")
#setup a connection to redis for the images database (Redis can have multiple databases)
redis_images = redis.Redis(host=redis_images_creds['hostname'], db=0, password=redis_images_creds['password'],
                           port=int(redis_images_creds['port']))

logger.debug("Connecting to Redis for RQ")
#Setup a connection that will be used by RQ (each redis connection instance only talks to 1 DB)
redis_queue = redis.Redis(
    host=redis_rq_creds['hostname'],
    db=0,
    password=redis_rq_creds['password'],
    port=int(redis_rq_creds['port'])
)

#Based on that connection, setup our job queue, and set async=True to tell it we want to run jobs out-of-band.
#We could set it to 'True' if we wanted it to run jobs right away.  Sometimes useful for debugging.
logger.debug("Setting up the queue")
q = Queue(connection=redis_queue, async=True)

# @job("dashboard", connection=redis_queue,timeout=10)  #When this is run as a job, use apecific queue (dashboard) with specific timeouts.
# def send_update(metric, value):
#     """
#     Accepts a metric name and a value, and sends it dweet.io
#     """
#     dweepy.dweet_for(configstuff['dweet_thing'], {metric: value})

def get_image(image_url):
    """
    This is the job that gets queued when a tweet needs to be analyzed
    """
    start = time.time()  #Lets store some timing info
    image = retrieve_image(image_url)  #Go get that image
    if image is not None:  #As long as we have a vaid image and its not None (aka Null)
        image = process_image(image)  #Do our image processings
        if configstuff['actually_store']:  #If our configu file says to store the image for reals
            key = store_to_vipr(image)  #store to vipr
            store_to_redis(key)  #and keep track of it in redis
    end = time.time()  # and record how long it took
    if random.randint(1, 10) < 5:  #about 50% of the time we should
        redis_queue.lpush("stats:execution-times", end - start)  #send in an update on execution time
    redis_queue.incr("stats:tweets-processed")  #and also record that we processed another tweet.

def store_to_redis(image_key):
    """
    Keep track of an image in redis
    """

    tx = redis_images.pipeline()  #Because we are goign to do a bunch of Redis ops quickly, setup a 'pipeline' (batch the ops)
    tx.hset(image_key.key, "filename",
            image_key.key)  #using the key's UUID as the name, set the hash value of 'filename' to the filename
    tx.hset(image_key.key, "url",
            image_key.generate_url(60 * 60 * 23))  #Get a URL, and store it.  That URL is good for 23 hrs
    tx.hset(image_key.key, "size",
            image_key.size)  #Store the size of the image.  Better to store it here where its cheap to check than in ViPR where its expensive.
    tx.expire(image_key.key, 60 * 60 * 23)  # Expire the entire redis key in 23 hours

    tx.execute()  #Run the transaction.
    logger.info("Stored image to redis: {}".format(image_key))

def process_image(image, random_sleep=1):
    image = image.filter(ImageFilter.BLUR)  #run the image through a blur filter
    final_image = StringIO()  #and now, since the PIL library requires a 'file like' object to store its data in, and I dont want to write a temp file, setup a stringIO to hold it.
    image.save(final_image, 'jpeg')  #store it as a JPG
    if random_sleep:  #added a random sleep here to make it seem like the process takes longer.  Simulates more expensive processing.
        time.sleep(random.randint(1, random_sleep))
    return final_image  #and give back the image

def retrieve_image(image_url):
    """
    Retrieves the image from the web
    """
    http = requests.session()
    logger.info("Capturing Image {}".format(image_url))
    im = None
    try:
        im = Image.open(StringIO(http.get(image_url).content))  #Try to get the image, but if it fails
    except IOError as e:
        logger.critical(e)  ##Record what happened and return a None
        return None
    logger.info("Image Captured: {}".format(im))
    return im

def store_to_vipr(image_data):
    logger.debug("Storing to ViPR")
    logger.debug("Connecting to ViPR")
    s3conn = boto.connect_s3(s3_creds['access_key'], s3_creds['secret_key'],
                             host=s3_creds['url'])  #set up an S3 style connections
    logger.debug("Getting bucket")
    bucket = s3conn.get_bucket(s3_creds['bucket_name'])  #reference to the S3 bucket.
    lifecycle = Lifecycle()  #new lifecycle managers
    logger.debug("Setting Bucket RulesViPR")
    lifecycle.add_rule('Expire 1 day', status='Enabled', expiration=Expiration(days=1))  #make sure the bucket it set to only allow 1 day old images.  Probably dont need to do this every time.  TODO!

    image_guid = str(uuid.uuid4())  #Pick a random UUID!
    k = Key(bucket)  #and gimme a new key to refer to file object.
    k.key = "{}.jpg".format(image_guid)  #give it a name based on the UUID.
    logger.debug("Uploading to ViPR")
    k.set_contents_from_string(image_data.getvalue())  #upload it.
    logger.info("Stored image {} to object store".format(k.key))
    return k  #and return that key info.

def watch_stream(every=10):
    hashtag = redis_queue.get("hashtag")
    twitter_api = TwitterAPI(
        consumer_key=twitter_creds['consumer_key'].encode('ascii','ignore'),
        consumer_secret=twitter_creds['consumer_secret'].encode('ascii','ignore'),
        access_token_key=twitter_creds['access_token'].encode('ascii','ignore'),
        access_token_secret=twitter_creds['token_secret'].encode('ascii','ignore')
    )  #setup the twitter streaming connectors.

    logger.info("Waiting for tweets...")
    while True:
        try:
            #tweet_stream = twitter_api.request('statuses/filter', {'track': hashtag})  #ask for a stream of statuses (1% of the full feed) that match my hash tags
            for tweet in twitter_api.request('statuses/filter', {'track': hashtag}).get_iterator():  #for each one of thise
                if hashtag != redis_queue.get("hashtag"):
                    logger.info("Hashtag changed from {}, breaking loop to restart with new hashtag".format(hashtag))
                    hashtag = redis_queue.get("hashtag")
                    break
                logger.info("Tweet Received: {}".format(hashtag))  #Log it
                redis_queue.incr("stats:tweets")  #Let Redis know we got another one.
                if tweet['entities']['media'][0]['type'] == 'photo': #Look for the photo.  If its not there, will throw a KeyError, caught below
                    logger.info("Dispatching tweet with URL {}".format(tweet['entities']['media'][0]['media_url']))  # log it
                    q.enqueue(
                        get_image,
                        tweet['entities']['media'][0]['media_url'],
                        timeout=60,
                        ttl=600
                    )  #add a job to the queue, calling get_image() with the image URL and a timeout of 60s
        except KeyError as e:
            logger.warn("Caught a key error for tweet, ignoring: {}".format(e.message))
        except Exception as e:
            logger.critical("UNEXPECTED EXCEPTION: {}".format(e))
