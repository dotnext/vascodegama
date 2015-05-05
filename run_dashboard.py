from dashboard.dashboard import update_dashboard
import time
import logging, logging.config, json
import utils

logging.config.dictConfig(utils.get_log_dict())
worker_logger = logging.getLogger("vascodagama.worker")
watcher_logger = logging.getLogger("vascodagama.watcher")



if __name__ == "__main__":
    while True:
        update_dashboard()
        time.sleep(5)
