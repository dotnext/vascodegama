
from rq.decorators import job #funtion decoration
import dweepy #see dweet.io
import logging, logging.config
import utils
import datetime

logging.config.dictConfig(utils.get_log_dict())

logger = logging.getLogger('vascodagama.dashboard')

from pyloggly import LogglyHandler


handler = LogglyHandler('d6985ec5-ebdc-4f2e-bab0-5163b1fc8f19', 'logs-01.loggly.com', 'dashboard')
logger.addHandler(handler)


redis_images = utils.get_images_redis_conn()
r = utils.get_rq_redis_conn()
q = utils.get_rq()

#Setup our redis and RQ connections.   see twitter_watch for more details.
configstuff = utils.configstuff()

@job("dashboard", connection=r, timeout=10, result_ttl=10)
def send_update(metric, value): #method for sending updates about metrics as needed.
    logger.debug("Sending update for {}: {}".format(metric, value))
    dweepy.dweet_for(configstuff['dweet_thing'], {metric: value})




def update_dashboard(): # the primary function.
    logger.info("updating")


    #For each one of the metrics, collected the data and issue a job to actually send that out.
    get_queue_len()
    # logging.debug("{}: {}".format("tweets in queue",tweets_in_queue))
    # send_update.delay("tweets-in-queue",tweets_in_queue)

    get_time_in_q()
    # logging.debug("{}: {}".format("time in q", time_in_q))
    # send_update.delay("longest-time-in-queue", time_in_q)

    get_worker_count()
    # logging.debug("{}: {}".format("workers", worker_count))
    # send_update.delay("worker-count",worker_count)



    get_ops_per_sec()
    # logging.debug("{}: {}".format("ops",ops_per_sec))
    # send_update.delay("ops",ops_per_sec)

    get_exec_time()
    # logging.debug("{}: {}".format("exec time", exec_time))
    # send_update.delay("execution-time",exec_time)

    get_tweet_count()
    # logging.debug("{}: {}".format("tweet count", tweet_count))
    # send_update.delay("tweet-count",tweet_count)

    get_tweets_processed()
    # logging.debug("{}: {}".format("processed count",tweets_processed))
    # send_update.delay("tweet-processed",tweets_processed)

    get_image_stats()
    # logging.debug("{}: {}".format("size",size))
    # send_update.delay("images-size", size)
    #
    # logging.debug("{}: {}".format("count",count))
    # send_update.delay("keys-in-redis", count)

def get_worker_count():

    count = len(r.keys("rq:worker:*"))
    logger.debug("Count: {}".format(count))
    send_update.delay("worker-count", count)

def get_image_stats():

    total_count = 0
    total_size = 0

    tx = redis_images.pipeline() #because this is going to be a bunch of operations, we do this in a pipeline.

    for key in redis_images.keys(): #for every key in Redis (yes, thats expensive and could be tens of thousands of keys)
        total_count += 1 #keep track of how many there are.
        tx.hget(key,"size") #and for each one get the size.

    for result in tx.execute(): #Now we execute that batch jobs, and the results come back in a list.  For each one
        if result:
            total_size += int(result) #Add it to the total size after converting to integer (Redis stores everything as a string)

    total_size = round(total_size / 1024 / 1024) #Turn that into MB

    logger.debug("Images: {}".format(total_size))
    logger.debug("Keys: {}".format(total_count))
    send_update.delay("images-size", total_size)
    send_update.delay("keys-in-redis", total_count)
    # return int(total_size), int(total_count) #and return both!

def get_ops_per_sec():
    total = int(redis_images.info()['instantaneous_ops_per_sec']) + int(r.info()['instantaneous_ops_per_sec'])
    logger.debug("Ops: {}".format(total))
    send_update.delay("ops",total)

def get_queue_len():
    logger.debug("Count: {}".format(len(q)))
    send_update.delay("tweets-in-queue",len(q))

def get_time_in_q():
    #logger.debug("enter time in q function")
    time = 0
    try:
        oldest_job = q.jobs[0] #find the oldest job (because its at queue position 0)
        enqueued = oldest_job.enqueued_at #find out when it was enqueues
        now = datetime.datetime.utcnow() #when is it now
        delta = (now-enqueued).seconds #whats the delta
        #logger.debug("exit time in q function as success: {}".format(delta))
        time = delta #and return it
    except IndexError as e:
        #Queue was empty if we got here, so the oldest value is 0
        #logger.debug("exit time in q function as error because no jobs")
        time = 0

    logger.debug("QueueTime: {}".format(time))
    send_update.delay("longest-time-in-queue", time)

def get_exec_time():
    count = r.llen("stats:execution-times") #We keep the execution times in a Redis List.  Found out how many entries we have
    if count == 0: #if there are none, return 0
        return 0.0
    total = 0 #other wise, for each value, add it up.
    for exec_time in r.lrange("stats:execution-times",0,count):
        total += float(exec_time)


    average = float(total)/count #and calculate the average.
    r.ltrim("stats:execution-times",0,0) #delete all the entries so its clean for next time.
    avg = round(average,2) #Return the average.
    logger.debug("Exec: {}".format(avg))
    send_update.delay("execution-time",avg)

def get_tweet_count():
    count = 0
    try:
        count = int(r.get("stats:tweets"))
    except:
        count =  0 #simple get from redis
    logger.debug("Tweet-Count: {}".format(count))
    send_update.delay("tweet-count",count)

def get_tweets_processed():
    proc = 0
    try:
        proc =  int(r.get("stats:tweets-processed")) #simple get from redis.
    except:
        proc =  0
    logger.debug("Tweet-Proc: {}".format(proc))
    send_update.delay("tweet-processed",proc)
