#some parts of RQ expect this file, but I dont want to include config details here, so I import them from my real config file
from config import Config
import os,json

redis_rq_creds = {}

if "VCAP_SERVICES" in os.environ:
    rediscloud = json.loads(os.environ['VCAP_SERVICES'])['rediscloud']
    for creds in rediscloud:
        if creds['name'] == "vascodagama-db":
            redis_rq_creds = creds['credentials']
else:
    cfg = Config(file('private_config_new.cfg'))
    redis_images_creds = cfg.redis_images_creds
    redis_rq_creds = cfg.redis_rq_creds

QUEUES = ['dashboard', 'default']
REDIS_URL = "redis://:{password}@{hostname}:{port}/0".format(password=redis_rq_creds['password'], hostname=redis_rq_creds['hostname'],port=int(redis_rq_creds['port']))

