from flask import Flask
from flask import Flask, request
import uuid
import os.path
import json

app = Flask(__name__)

filearea = None
index = None

@app.route('/')
def hello_world():
    return 'Hello, World!'

def from_datetime(value):
    return datetime.datetime.strptime(value, '%Y-%m-%dT%H:%M:%S.%fZ')

@app.route('/index/lookup')
def lookup():
    args = dict(request.args)
    pretty = args.pop("pretty", False)
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
    args = {name: argtypes[name][0](value) if name in args else argtypes[name][1]
            for name, value in args.items()}
    
    if pretty:
        return json.dumps(index.lookup(**args), indent=2)
    else:
        return "".join(
            json.dumps(row) + "\n"
            for row in index.lookup(**args))
    
@app.route('/index/add', methods=["POST"])
def add_file():
    filename = os.path.join(filearea, "%s.grb" % (uuid.uuid4(),))
    with open(filename, "wb") as f:
        f.write(request.get_data())
    parametermap = request.args.get("parametermap", None)
    index.add_file(filename, parametermap = parametermap)
    return json.dumps({"status": "success"})

if __name__ == "__main__":
    app.run()
