from flask import Flask

from functools import wraps
from flask import request, Response, jsonify
import logging
import os
from cloudfoundry import CloudFoundryInterface
from config import Config

cfg = Config(file('private_config.cfg'))

logging.basicConfig(level=logging.DEBUG)



def check_auth(username, password):
    """This function is called to check if a username /
    password combination is valid.
    """
    return username == 'test' and password == 'emc'


def authenticate():
    """Sends a 401 response that enables basic auth"""
    return Response(
        'Could not verify your access level for that URL.\n'
        'You have to login with proper credentials', 401,
        {'WWW-Authenticate': 'Basic realm="Login Required"'})


def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return authenticate()
        return f(*args, **kwargs)

    return decorated


app = Flask(__name__)


@app.route('/')
@requires_auth
def index():
    logging.info("Got request for login test")
    return jsonify({'status': 'success'})


@app.route('/apps')
@requires_auth
def apps():
    logging.info("Got request for apps")
    cfi = CloudFoundryInterface('https://api.run.pivotal.io', username=cfg.cf_user, password=cfg.cf_pass)
    cfi.login()

    app_list = [cf_app.name for cf_app in cfi.apps.itervalues()]
    applications = {'applications': sorted(app_list)}
    return jsonify(applications)


@app.route('/scale/<appname>/<int:target>')
@requires_auth
def scale_app(appname, target):
    logging.info("Got request to scale app \'{}\' to {}".format(appname, target))
    cfi = CloudFoundryInterface('https://api.run.pivotal.io', username=cfg.cf_user, password=cfg.cf_pass)
    cfi.login()

    app_list = [cf_app.name for cf_app in cfi.apps.itervalues()]
    if appname not in app_list:
        return jsonify({'status': 'failure'})
    else:
        cfi.scale_app(cfi.get_app_by_name(appname),target)
        return jsonify({"status":"success"})



if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv('VCAP_APP_PORT', '5000')))