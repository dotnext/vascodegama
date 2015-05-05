import time
import random
import logging, logging.config, json
import requests
import boto
import uuid
from TwitterAPI import TwitterAPI
from boto.s3.key import Key
from StringIO import StringIO
from PIL import ImageFilter, Image
import httplib
from requests.packages.urllib3.exceptions import ProtocolError
import utils


logging.config.dictConfig(utils.get_log_dict())
worker_logger = logging.getLogger("vascodagama.worker")
watcher_logger = logging.getLogger("vascodagama.watcher")


def get_image(image_url, actually_store=True):
    """
    This is the job that gets queued when a tweet needs to be analyzed
    """
    redis_queue = utils.get_rq_redis_conn()

    start = time.time()  #Lets store some timing info
    image = retrieve_image(image_url)  #Go get that image
    if image is not None:  #As long as we have a vaid image and its not None (aka Null)
        image = process_image(image)  #Do our image processings
        if actually_store:  #If our configu file says to store the image for reals
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
    redis_images = utils.get_images_redis_conn()
    tx = redis_images.pipeline()  #Because we are goign to do a bunch of Redis ops quickly, setup a 'pipeline' (batch the ops)
    tx.hset(image_key.key, "filename",
            image_key.key)  #using the key's UUID as the name, set the hash value of 'filename' to the filename
    tx.hset(image_key.key, "url",
            image_key.generate_url(60 * 60 * 23))  #Get a URL, and store it.  That URL is good for 23 hrs
    tx.hset(image_key.key, "size",
            image_key.size)  #Store the size of the image.  Better to store it here where its cheap to check than in ViPR where its expensive.
    tx.expire(image_key.key, 60 * 60 * 23)  # Expire the entire redis key in 23 hours

    tx.execute()  #Run the transaction.
    worker_logger.info("Stored image to redis: {}".format(image_key))

def process_image(image, random_sleep=0):
    worker_logger.info("Processing image")
    image = image.filter(ImageFilter.FIND_EDGES)  #run the image through a blur filter
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
    worker_logger.info("Capturing Image {}".format(image_url))
    im = None
    try:
        im = Image.open(StringIO(http.get(image_url).content))  #Try to get the image, but if it fails
    except IOError as e:
        worker_logger.critical(e)  ##Record what happened and return a None
        return None
    worker_logger.info("Image Captured: {}".format(im))
    return im

def store_to_vipr(image_data):
    s3_creds = utils.s3_creds()
    worker_logger.debug("Storing to ViPR")
    worker_logger.debug("Connecting to ViPR")
    s3conn = boto.connect_s3(s3_creds['access_key'], s3_creds['secret_key'],
                             host=s3_creds['url'])  #set up an S3 style connections
    worker_logger.debug("Getting bucket")
    bucket = s3conn.get_bucket(s3_creds['bucket_name'])  #reference to the S3 bucket.
    #lifecycle = Lifecycle()  #new lifecycle managers
    #worker_logger.debug("Setting Bucket RulesViPR")
    #lifecycle.add_rule('Expire 1 day', status='Enabled', expiration=Expiration(days=1))  #make sure the bucket it set to only allow 1 day old images.  Probably dont need to do this every time.  TODO!

    image_guid = str(uuid.uuid4())  #Pick a random UUID!
    k = Key(bucket)  #and gimme a new key to refer to file object.
    k.key = "{}.jpg".format(image_guid)  #give it a name based on the UUID.
    worker_logger.debug("Uploading to ViPR")
    k.set_contents_from_string(image_data.getvalue())  #upload it.
    worker_logger.info("Stored image {} to object store".format(k.key))
    return k  #and return that key info.

def watch_stream():
    twitter_creds = utils.twitter_creds()
    redis_queue = utils.get_rq_redis_conn()
    hashtag = redis_queue.get("hashtag")
    q = utils.get_rq()

    twitter_api = TwitterAPI(
        consumer_key=twitter_creds['consumer_key'].encode('ascii','ignore'),
        consumer_secret=twitter_creds['consumer_secret'].encode('ascii','ignore'),
        access_token_key=twitter_creds['access_token'].encode('ascii','ignore'),
        access_token_secret=twitter_creds['token_secret'].encode('ascii','ignore')
    )  #setup the twitter streaming connectors.

    watcher_logger.info("Waiting for tweets...")
    while True:
            try:
                for tweet in twitter_api.request('statuses/filter', {'track': hashtag}).get_iterator():  #for each one of thise
                    if hashtag != redis_queue.get("hashtag"):
                        watcher_logger.info("Hashtag changed from {}, breaking loop to restart with new hashtag".format(hashtag))
                        hashtag = redis_queue.get("hashtag")
                        break
                    #watcher_logger.debug("Tweet Received: {}".format(hashtag))  #Log it
                    redis_queue.incr("stats:tweets")  #Let Redis know we got another one.
                    watcher_logger.debug("received tweet with tag {}".format(hashtag))
                    try:
                        if tweet['entities']['media'][0]['type'] == 'photo': #Look for the photo.  If its not there, will throw a KeyError, caught below
                            if 'retweeted' not in tweet:
                                watcher_logger.info("Tweet was a RT - ignoring")
                                continue
                            watcher_logger.info("Dispatching tweet ({}) with URL {}".format(hashtag,tweet['entities']['media'][0]['media_url']))  # log it
                            q.enqueue(
                                get_image,
                                tweet['entities']['media'][0]['media_url'],
                                ttl=60,
                                result_ttl=60,
                                timeout=60
                            )  #add a job to the queue, calling get_image() with the image URL and a timeout of 60s
                    except KeyError as e:
                        watcher_logger.debug("Caught a key error for tweet, expected behavior, so ignoring: {}".format(e.message))
                    except Exception as e:
                        watcher_logger.critical("UNEXPECTED EXCEPTION: {}".format(e))
            except httplib.IncompleteRead as e:
                watcher_logger.warn("HTTP Exception {}".format(e))
            except ProtocolError as e:
                watcher_logger.warn("Protocol Exception {}".format(e))
