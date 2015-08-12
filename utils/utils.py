from __future__ import print_function  # force the use of print(X) rather than print X


_redis_rq_creds = None
_redis_images_creds = None
_s3_creds = None
_twitter_creds = None
_configstuff = None
_logging_config = None
_redis_images = None
_redis_queue = None
_q = None

valid_config = False

def _get_config():
    import os, json
    global _redis_rq_creds
    global _redis_images_creds
    global _s3_creds
    global _twitter_creds
    global _configstuff
    global _logging_config
    global _redis_images
    global _redis_queue
    global _q
    global valid_config


    _logging_config = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'standard': {
                'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
            },
        },
        'handlers': {
            'default': {
                'level':'DEBUG',
                'class':'logging.StreamHandler',
            },
        },
        'loggers': {
            '': {
                'handlers': ['default'],
                'level': 'DEBUG',
                'propagate': True
            },
        }
    }

    if "VCAP_SERVICES" in os.environ:
        userservices = json.loads(os.environ['VCAP_SERVICES'])['user-provided']
        for configs in userservices:

            if configs['name'] == "s3_storage":
                _s3_creds = configs['credentials']
            elif configs['name'] == "twitter":
                _twitter_creds = configs['credentials']
            elif configs['name'] == "configstuff":
                _configstuff = configs['credentials']
            # elif configs['name'] == "logging_config":
            #     _logging_config = configs['credentials']
            elif configs['name'] == "redis-rq":
                _redis_rq_creds = configs['credentials']
            elif configs['name'] == "redis-metadata":
                _redis_images_creds = configs['credentials']


        valid_config = True

if not valid_config:
    _get_config()


def get_log_dict():
    return _logging_config

def twitter_creds():
    return _twitter_creds

def redis_rq_creds():
    return _redis_rq_creds

def redis_images_creds():
    return _redis_images_creds

def configstuff():
    return _configstuff

def logging_config():
    return _logging_config


def s3_creds():
    return _s3_creds


def get_images_redis_conn():
    import redis  # redis library
    global _redis_images
    if _redis_images is None:
        _redis_images = redis.Redis(host=_redis_images_creds['host'], db=0, password=_redis_images_creds['pass'],port=int(_redis_images_creds['port']))
    return _redis_images

def get_rq_redis_conn():
    import redis  # redis library
    global _redis_queue
    if _redis_queue is None:
        _redis_queue = redis.Redis(
            host=_redis_rq_creds['host'],
            db=0,
            password=_redis_rq_creds['pass'],
            port=int(_redis_rq_creds['port'])
        )
    return _redis_queue

def get_rq():
    global _q
    if _q is None:
        from rq import Queue  # RQ, the job queueing system we use
        _q = Queue(connection=get_rq_redis_conn(), async=True)
    return _q
