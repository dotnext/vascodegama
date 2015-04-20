
from rq.decorators import job #funtion decoration
import dweepy #see dweet.io
import logging, logging.config
import utils
import datetime

logging.config.dictConfig(utils.get_log_dict())

logger = logging.getLogger('vascodagama.dashboard')


redis_images = utils.get_images_redis_conn()
r = utils.get_rq_redis_conn()
q = utils.get_rq()

#Setup our redis and RQ connections.   see twitter_watch for more details.
configstuff = utils.configstuff()

@job("dashboard", connection=r, timeout=10, result_ttl=10)
def send_update(metric, value): #method for sending updates about metrics as needed.
    logger.info("Sending update for {}: {}".format(metric, value))
    dweepy.dweet_for(configstuff['dweet_thing'], {metric: value})

def update_dashboard(): # the primary function.
    logger.info("updating")


    #For each one of the metrics, collected the data and issue a job to actually send that out.
    tweets_in_queue = get_queue_len()
    logging.debug("{}: {}".format("tweets in queue",tweets_in_queue))
    send_update.delay("tweets-in-queue",tweets_in_queue)
    #dog.metric('tweets.inqueue', tweets_in_queue, host="twitter")

    time_in_q = get_time_in_q()
    logging.debug("{}: {}".format("time in q", time_in_q))
    send_update.delay("longest-time-in-queue", time_in_q)
    #dog.metric('tweets.timeinqueue', time_in_q, host="twitter")

    worker_count = get_worker_count()
    logging.debug("{}: {}".format("workers", worker_count))
    send_update.delay("worker-count",worker_count)
    #dog.metric('workers.count', worker_count, host="twitter")



    ops_per_sec = get_ops_per_sec()
    logging.debug("{}: {}".format("ops",ops_per_sec))
    send_update.delay("ops",ops_per_sec)
    #dog.metric('redis.inqueue', ops_per_sec, host="twitter")

    exec_time = get_exec_time()
    logging.debug("{}: {}".format("exec time", exec_time))
    send_update.delay("execution-time",exec_time)
    #dog.metric('tweets.execution_time', exec_time, host="twitter")

    tweet_count = get_tweet_count()
    logging.debug("{}: {}".format("tweet count", tweet_count))
    send_update.delay("tweet-count",tweet_count)
    #dog.metric('tweets.count', tweet_count, host="twitter")

    tweets_processed = get_tweets_processed()
    logging.debug("{}: {}".format("processed count",tweets_processed))
    send_update.delay("tweet-processed",tweets_processed)
    #dog.metric('tweets.processed', tweets_processed, host="twitter")

    size, count = get_image_stats()
    logging.debug("{}: {}".format("size",size))
    send_update.delay("images-size", size)
    #dog.metric('images.size', size, host="twitter")

    logging.debug("{}: {}".format("count",count))
    send_update.delay("keys-in-redis", count)
    #dog.metric('images.count', count, host="twitter")

def get_worker_count():

    return len(r.keys("rq:worker:*"))

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

    return int(total_size), int(total_count) #and return both!

def get_ops_per_sec():
    total = int(redis_images.info()['instantaneous_ops_per_sec']) + int(r.info()['instantaneous_ops_per_sec'])
    return total #Get this from the Redis status

def get_queue_len():
    #return 22
    return len(q) #Easy way to determine number of jobs in queue.

def get_time_in_q():
    logger.debug("enter time in q function")
    try:
        oldest_job = q.jobs[0] #find the oldest job (because its at queue position 0)
        enqueued = oldest_job.enqueued_at #find out when it was enqueues
        now = datetime.datetime.utcnow() #when is it now
        delta = (now-enqueued).seconds #whats the delta
        logger.debug("exit time in q function as success: {}".format(delta))
        return delta #and return it
    except IndexError as e:
        #Queue was empty if we got here, so the oldest value is 0
        logger.debug("exit time in q function as error because no jobs")
        return 0

def get_exec_time():
    count = r.llen("stats:execution-times") #We keep the execution times in a Redis List.  Found out how many entries we have
    if count == 0: #if there are none, return 0
        return 0.0
    total = 0 #other wise, for each value, add it up.
    for exec_time in r.lrange("stats:execution-times",0,count):
        total += float(exec_time)


    average = float(total)/count #and calculate the average.
    r.ltrim("stats:execution-times",0,0) #delete all the entries so its clean for next time.
    return round(average,2) #Return the average.

def get_tweet_count():
    try:
        return int(r.get("stats:tweets"))
    except:
        return 0 #simple get from redis

def get_tweets_processed():
    try:
        return int(r.get("stats:tweets-processed")) #simple get from redis.
    except:
        return 0
