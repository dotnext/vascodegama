
from pprint import pprint
from config import Config
import logging, logging.config
from logging_tree import printout
import utils

cfg = Config(file("private_config_new.cfg"))

logging.config.dictConfig(utils.get_log_dict())
printout()