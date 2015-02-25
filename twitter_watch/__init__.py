from __future__ import print_function
from config import Config
from TwitterAPI import TwitterAPI
from PIL import ImageFilter, Image
from StringIO import StringIO
import requests
from pprint import pprint,pformat
import logging
import boto
from boto.s3.key import Key
import time
import redis
import uuid

import random
from log_with import log_with
from rq import Queue
from rq.decorators import job

logger = logging.getLogger('')
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s [%(module)s:%(funcName)s] [%(levelname)-5.5s] %(message)s")
# loggly_handler = loggly.handlers.HTTPSHandler(url="{}{}".format(credentials["Loggly"]["url"], "gui"))
# loggly_handler.setLevel(logging.DEBUG)
# logger.addHandler(loggly_handler)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(formatter)
logger.addHandler(ch)
logging.getLogger("requests.packages.urllib3.connectionpool").setLevel(logging.WARN)
import dweepy

cfg = Config(file('private_config.cfg'))

redis_images = redis.Redis(host=cfg.redis_host,db=cfg.redis_images_db, password=cfg.redis_password)
r = redis.Redis(
    host=cfg.redis_host,
    db=cfg.redis_rq_db,
    password=cfg.redis_password
)

q = Queue(connection=r)

@job("dashboard",connection=r,timeout=3)
def send_update(metric, value):
    dweepy.dweet_for(cfg.dweet_thing, {metric: value})


@log_with(logger)
def get_image(image_url):
    start = time.time()
    image = retrieve_image(image_url)
    if image is not None:
        image = process_image(image)
        if cfg.actually_store == True:
            key = store_to_vipr(image)
            store_to_redis(key)
    end = time.time()
    if random.randint(1,10) == 1:
        send_update.delay("execution-time",round((end-start),2))


@log_with(logger)
def store_to_redis(image_key):
    url = image_key.generate_url(1200)
    redis_images.set(image_key.key,image_key.generate_url(1200))
    logger.info("Stored image to redis: {}".format(image_key))


@log_with(logger)
def process_image(image, random_sleep=5):
    # for x in range(1, random.randint(10, 100)):
    #     # logger.debug("Image Filter Pass {}".format(x))
    #     image = image.filter(ImageFilter.BLUR)
    image = image.filter(ImageFilter.BLUR)
    final_image = StringIO()
    image.save(final_image, 'jpeg')
    if random_sleep:
        time.sleep(random.randint(1, random_sleep))
    return final_image


@log_with(logger)
def retrieve_image(image_url):
    http = requests.session()
    logger.info("Capturing Image {}".format(image_url))
    im = None
    try:
        im = Image.open(StringIO(http.get(image_url).content))
    except IOError as e:
        logger.critical(e)
        return None
    logger.info("Image Captured: {}".format(im))
    return im


@log_with(logger)
def store_to_vipr(image_data):
    s3conn = boto.connect_s3(cfg.vipr_access_key, cfg.vipr_secret_key, host=cfg.vipr_url)
    bucket = s3conn.get_bucket(cfg.vipr_bucket_name)
    image_guid = str(uuid.uuid4())
    k = Key(bucket)
    k.key = "{}.jpg".format(image_guid)
    k.set_contents_from_string(image_data.getvalue())
    logger.info("Stored image {} to object store".format(k.key))
    return k


@log_with(logger)
def watch_stream(every=10):
    twitter_api = TwitterAPI(
        consumer_key=cfg.twitter_consumer_key,
        consumer_secret=cfg.twitter_consumer_secret,
        access_token_key=cfg.twitter_access_token,
        access_token_secret=cfg.twitter_token_secret
    )
    tweet_count = 0
    r = twitter_api.request('statuses/filter', {'track': cfg.hashtag})
    #r = twitter_api.request('statuses/filter', {'locations': '-74,40,-73,41'})
    for tweet in r.get_iterator():
        tweet_count += 1
        logger.info("{}: Tweet Received".format(tweet_count))
        if tweet_count % every == 0:
            send_update("tweet-count",tweet_count)
        if not tweet['retweeted'] and 'entities' in tweet and 'media' in tweet['entities'] and \
                        tweet['entities']['media'][0]['type'] == 'photo':
            logger.info("Dispatching tweet with URL {}".format(tweet['entities']['media'][0]['media_url']))
            q.enqueue(
                get_image,
                tweet['entities']['media'][0]['media_url'],
                timeout=60,
                ttl=600
            )
