#some parts of RQ expect this file, but I dont want to include config details here, so I import them from my real config file

import os
import json


QUEUES = ['dashboard', 'default']
REDIS_URL = json.loads(os.environ['VCAP_SERVICES'])['user-provided'][0]['credentials']['string']


#REDIS_URL = json.loads(os.environ(['VCAP_SERVICES']))