from twitter_watch.twitter_watch import watch_stream
import logging, logging.config
import json
from config import Config
import os


logging_config = None

cfg = None
if "VCAP_SERVICES" in os.environ:

    userservices = json.loads(os.environ['VCAP_SERVICES'])['user-provided']
    for configs in userservices:
        if configs['name'] == "logging_config":
            logging_config = json.loads(configs['credentials'])
else:
    print("Loading logging config from file")
    cfg = Config(file('private_config_new.cfg'))
    logging_config = json.loads(cfg.logging_config)


if __name__ == "__main__":
    logging.config.dictConfig(logging_config)
    watch_stream()
