from twitter_watch.twitter_watch import watch_stream
import logging, logging.config
import json
import utils

logging.config.dictConfig(utils.get_log_dict())

logger = logging.getLogger()

if __name__ == "__main__":
    watch_stream()
