from flask import Flask, render_template

import logging

import redis
from config import Config
import os

cfg = Config(file('private_config.cfg'))
logger = logging.getLogger('')
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s [%(module)s:%(funcName)s] [%(levelname)-5.5s] %(message)s")
# loggly_handler = loggly.handlers.HTTPSHandler(url="{}{}".format(credentials["Loggly"]["url"], "gui"))
# loggly_handler.setLevel(logging.DEBUG)
# logger.addHandler(loggly_handler)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(formatter)
logger.addHandler(ch)
logging.getLogger("requests.packages.urllib3.connectionpool").setLevel(logging.WARN)


app = Flask(__name__)

redis_images = redis.Redis(host=cfg.redis_host,db=cfg.redis_images_db, password=cfg.redis_password)

def get_random_urls(count=100):
    pipe_keys = redis_images.pipeline()
    pipe_urls = redis_images.pipeline()
    keys = []
    for i in range(0,count):
        pipe_keys.randomkey()

    for key in pipe_keys.execute():
        pipe_urls.get(key)

    urls = pipe_urls.execute()
    return urls

@app.route('/')
def dashboard():
    urls = get_random_urls()
    return render_template('default-us.html',urls=urls)

if __name__ == "__main__":
    port = int(os.getenv('VCAP_APP_PORT', '5000'))
    logging.info("Running on port {}".format(port))
    app.run(host='0.0.0.0', port=port)