import argparse
import importlib
import inspect
import logging
import os
import sys
import time
from collections import Counter

import confuse
from flask import Flask, request, jsonify

app = Flask(__name__)

TIKIDO_PLUGINS = {}
for plugin_folder in os.listdir(os.path.join(os.path.dirname(__file__), 'plugins')):
    if not plugin_folder.startswith('_') and os.path.isdir(os.path.join(os.path.dirname(__file__), 'plugins') + os.sep + plugin_folder):

        mod = importlib.import_module('plugins.' + plugin_folder)
        meta_data = getattr(mod, 'meta', None)
        if meta_data and 'name' in meta_data:
            TIKIDO_PLUGINS[meta_data['name']] = dict(folder=plugin_folder, freshness=int(time.time()))

parameters = confuse.Configuration('TiKido-agent', __name__)
parameters.set_file('agent_config.yaml')
#
arg_parser = argparse.ArgumentParser()
arg_parser.add_argument('--user_config', '-uc', action="store", default='agent_user_config.yaml',
                        help="User's agent configuration")
#
parameters.set_args(arg_parser.parse_args())
#
if parameters['user_config']:
    parameters.set_file(parameters['user_config'].get())
#
port = parameters['port'].get() if 'port' in parameters else 8080
host = parameters['host'].get() if 'host' in parameters else '0.0.0.0'
log = logging.getLogger(__name__)
logging.getLogger('').setLevel(logging.DEBUG)

counter = Counter()
try:
    import rainbow_logging_handler

    handler = rainbow_logging_handler.RainbowLoggingHandler(sys.stderr)
    handler._column_color['%(asctime)s'] = ('cyan', None, False)
    handler._column_color['%(levelname)-7s'] = ('green', None, False)
    handler._column_color['%(message)s'][logging.INFO] = ('white', None, False)

    handler.setFormatter(logging.Formatter("%(levelname)-5s %(message)s"))

    root = logging.getLogger("")
    root.addHandler(handler)
except Exception as e:
    print(e)
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    formatter = logging.Formatter('\n%(levelname)-8s %(name)-12s %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)


def return_success(**kwargs):
    return dict(result='success', value='success', vars=kwargs.get('vars', {}))


def return_error(*args):
    msg = inspect.stack()[1].function if len(args) == 1 else args[0]
    err_msg = f'cannot {msg} - %r' % args[-1]
    log.exception(err_msg)
    return dict(result='error', value=msg, vars={args})


@app.errorhandler(403)
def mistake403():
    return 'There is a mistake in your url!'


@app.errorhandler(404)
def mistake404():
    return 'Sorry, this page does not exist!'


@app.route('/monitor/<code>', methods=['GET'])
def handler(code):
    started = time.time()
    try:
        input_json = request.json
        if input_json is None:
            raise Exception('input json is None')
        plug = input_json['plugin']
        action_name = input_json['action']
        opts = input_json['options']
        log.info(f"plug: {plug}, action_name: {action_name}, opts: {opts}")

        log.debug(f'received job: {input_json}')
        full_module_name = f"plugins.{TIKIDO_PLUGINS[plug]['folder']}.logic"
        importlib.import_module(full_module_name)

        logic = sys.modules[full_module_name].Logic(options=opts)
        ret = logic.action(action_name, **input_json['action_args'])
        if ret['result'] in {'success', 'error', 'OK'}:
            log.debug(f'{(time.time() - started):f}. {action_name} job done. ret: {ret}')
            counter[code] += 1
            return jsonify(ret)
    except Exception as err:
        return return_error(err)


@app.route('/monitor/statistics', methods=['GET'])
def handler_post():
    try:
        return counter
    except Exception as e:
        log.exception(e)


@app.route('/')
@app.route('/<path:path>')
def other_page(path=''):
    log.debug(path)
    return jsonify({'result': 'OK'})


if __name__ == '__main__':
    app.run(port=port, ssl_context=('127.0.0.1.crt', 'device.key'))
