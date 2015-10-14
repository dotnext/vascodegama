#some parts of RQ expect this file, but I dont want to include config details here, so I import them from my real config file

import os
import json


QUEUES = ['dashboard', 'default']

REDIS_STUFF = json.loads(os.environ['VCAP_SERVICES'])['user-provided'][0]['credentials']

print REDIS_STUFF

REDIS_URL = "redis://:{password}@{hostname}:{port}".format(password=REDIS_STUFF['pass'], hostname=REDIS_STUFF['host'], port=int(REDIS_STUFF['port']))

print REDIS_URL

#REDIS_URL = json.loads(os.environ(['VCAP_SERVICES']))
