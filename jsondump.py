import json,os
from config import Config

cfg = Config(file("private_config_new.cfg"))

d = {
    "configstuff" : dict(cfg.configstuff),
    "s3_creds" : dict(cfg.s3_creds),
    "twitter_creds" : dict(cfg.twitter_creds),
}

with open("config.json", "w") as f:
    json.dump(dict(d), f)

