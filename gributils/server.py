from flask import Flask
from flask import Flask, request
import uuid
import os.path
import json
import datetime

app = Flask(__name__)

filearea = None
index = None

def format_result(result, pretty):
    if pretty:
        return json.dumps(result, indent=2)
    else:
        return "".join(
            json.dumps(row) + "\n"
            for row in result)

@app.route('/')
def hello_world():
    return 'Hello, World!'

def from_datetime(value):
    return datetime.datetime.strptime(value, '%Y-%m-%dT%H:%M:%S.%fZ')

@app.route('/index/lookup')
def lookup():
    args = dict(request.args)
    argtypes = {
        "output": (str, "layers"),
        "timestamp": (from_datetime, None),
        "timestamp-last-before": (int, 1),
        "parameter-name": (str, None),
        "parameter-unit": (str, None),
        "type-of-level": (str, None),
        "level": (float, None),
        "level-highest-below": (bool, False),
        "lat": (float, None),
        "lon": (float, None),
        "pretty": (bool, False)
    }
    args = {name.replace("-", "_"): argtypes[name][0](args[name]) if name in args else argtypes[name][1]
            for name in argtypes.keys()}
    pretty = args.pop("pretty", False)
    return format_result(index.lookup(**args), pretty)

@app.route('/index/interpolate/latlon')
def interp_latlon():
    args = dict(request.args)
    argtypes = {
        'gribfile': (str, None),
        'layeridx': (int, 1),
        'lat': (float, None),
        'lon': (float, None)
    }
    args = {name.replace("-", "_"): argtypes[name][0](args[name]) if name in args else argtypes[name][1]
            for name in argtypes.keys()}
    return json.dumps(index.interp_latlon(**args))


@app.route('/index/interpolate/timestamp')
def interp_timestamp():
    args = dict(request.args)
    argtypes = {
        'timestamp': (from_datetime, None),
        'parameter-name': (str, None),
        'parameter-unit': (str, None),
        'type-of-level': (str, None),
        'level': (float, None),
        'level-highest-below': (bool, False),
        'lat': (float, None),
        'lon': (float, None),
        "pretty": (bool, False)
    }
    args = {name.replace("-", "_"): argtypes[name][0](args[name]) if name in args else argtypes[name][1]
            for name in argtypes.keys()}
    pretty = args.pop("pretty", False)
    return format_result(index.interp_timestamp(**args), pretty)

@app.route('/index/add', methods=["POST"])
def add_file():
    filename = os.path.join(filearea, "%s.grb" % (uuid.uuid4(),))
    with open(filename, "wb") as f:
        f.write(request.get_data())
    parametermap = request.args.get("parametermap", None)
    index.add_file(filename, parametermap = parametermap)
    return json.dumps({"status": "success"})

@app.route('/index/parametermap/add', methods=["POST"])
def parametermap_add_file():
    filename = os.path.join(filearea, "%s.csv" % (uuid.uuid4(),))
    with open(filename, "wb") as f:
        f.write(request.get_data())
    index.add_parametermap(mapping=filename, name=request.args.get("name"))
    return json.dumps({"status": "success"})
    
@app.route('/index/parametermap')
def parametermap_list():
    return json.dumps(index.get_parametermaps())

if __name__ == "__main__":
    app.run()
