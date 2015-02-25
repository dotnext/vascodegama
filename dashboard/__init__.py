import requests
from config import Config
import json
import time
import logging
import redis
from log_with import log_with, dump_args
from memoize import Memoizer
from rq import Queue, Worker
from rq.decorators import job
import datetime


cache_store = {}
memo = Memoizer(cache_store)
max_cache_time = 2


logger = logging.getLogger('')
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s [%(module)s:%(funcName)s] [%(levelname)-5.5s] %(message)s")
# loggly_handler = loggly.handlers.HTTPSHandler(url="{}{}".format(credentials["Loggly"]["url"], "gui"))
# loggly_handler.setLevel(logging.DEBUG)
# logger.addHandler(loggly_handler)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)
logging.getLogger("requests.packages.urllib3.connectionpool").setLevel(logging.WARN)
import dweepy

cfg = Config(file('private_config.cfg'))
r = redis.Redis(
    host=cfg.redis_host,
    db=cfg.redis_rq_db,
    password=cfg.redis_password
)

q = Queue(connection=r)


@job("dashboard", connection=r, timeout=3)
def send_update(metric, value):
    logger.info("Sending update for {}".format(metric))
    dweepy.dweet_for(cfg.dweet_thing, {metric: value})

def update_dashboard():
    logger.info("updating")
    logger.info("Sending update for {}".format("tweets-in-queue"))
    send_update.delay("tweets-in-queue",get_queue_len())
    #send_update("qlengraph", get_queue_len())
    logger.info("Sending update for {}".format("time-in-queue"))
    send_update.delay("longest-time-in-queue", get_time_in_q())
    #send_update("qtimegraph", get_time_in_q())

    send_update.delay("worker-count",get_worker_count())

def get_worker_count():

    return len(r.smembers("rq:workers"))-1

def get_queue_len():
    return len(q)


def get_time_in_q():

    try:
        oldest_job = q.jobs[0]
        enqueued = oldest_job.enqueued_at
        now = datetime.datetime.utcnow()
        delta = (now-enqueued).seconds
        return delta
    except IndexError as e:
        #Queue was empty!
        return 0

def get_exec_time():
    return 0

