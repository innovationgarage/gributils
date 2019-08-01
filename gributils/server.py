from flask import Flask
from flask import Flask, request
import uuid
import os.path
import json
import datetime
import urllib.parse
import flask
import flask_swagger

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

def argparse(request):
    def parseitem(item):
        item = urllib.parse.unquote(item)
        try:
            return datetime.datetime.strptime(item, '%Y-%m-%dT%H:%M:%S.%fZ')
        except:
            try:
                return json.loads(item)
            except:
                return item
    return {parseitem(key).replace("-", "_"): parseitem(value)
            for key, value in request.args.items()}

def from_datetime(value):
    return datetime.datetime.strptime(value, '%Y-%m-%dT%H:%M:%S.%fZ')

@app.route("/")
def spec():
    resp = flask.make_response(flask.jsonify(flask_swagger.swagger(app)))
    resp.headers.set('Access-Control-Allow-Origin', '*')
    return resp

@app.route('/index/lookup')
def lookup():
    """
    Return a set of griblayers matching the specified requirements
    ---
    produces:
    - "application/json"
    parameters:
    - name: output
      in: query
      description: Output type selector
      required: true
      type: string
      enum:
        - layers
        - names
        - units
        - level-types
        - levels
    - name: lat
      in: query
      description: Latitude for filtering on layers covering a point
      type: number
    - name: lon
      in: query
      description: Longitude for filtering on layers covering a point
      type: number
    - name: timestamp
      in: query
      description: Timestamp for filtering on layers close to a certain time
      type: string
      format: "Date time: %Y-%m-%dT%H:%M:%S.%fZ"
    - name: parameter_name
      in: query
      description: Parameter name for filtering on layers containing only a certain parameter value such as "Wind speed"
      type: string
    - name: parameter_unit
      in: query
      description: Parameter unit name for filtering on layers containing only parameter values in a certain unit, such as m/s
      type: string
    - name: type_of_level
      in: query
      description: Type of level for filtering on only layers with a specified level of this type, such as "Meters above sea level"
      type: string
    - name: level
      in: query
      description: Level for filtering on only layers at this level. Combine with type_of_level to specify layers at e.g. 10m above sea level.
      type: number
    - name: timestamp_last_before
      in: query
      description: Find the last layer before the specified timestamp (1), or the first one after that timestamp (0)
      type: integer
      default: 1
    - name: level_highest_below
      in: query
      description: Find the layer at the highest level under the specified level (1) or at the lowest level above that level (0)
      type: integer
      default: 1
    - name: pretty
      in: query
      description: Pretty-print a single json object (true) or return newline separated json
      type: string
      enum:
        - true
    responses:
      200:
        description: "A set of layers"
    """
    args = argparse(request)
    pretty = args.pop("pretty", False)
    return format_result(index.lookup(**args), pretty)

@app.route('/index/interpolate/latlon')
def interp_latlon():
    """
    Interpolate a parametervalue at a specific lat/lon inside a specified layer
    ---
    produces:
    - "application/json"
    parameters:
    - name: gribfile
      in: query
      description: The gribfile id of the layer
      required: true
      type: string
    - name: layeridx
      in: query
      description: The layer index (starts with 1 for the first layer)
      required: true
      type: integer
    - name: lat
      in: query
      description: Latitude for the point
      required: true
      type: number
    - name: lon
      in: query
      description: Longitude for for the point
      required: true
      type: number
    responses:
      200:
        description: "An interpolated parameter value (float)"
    """
    args = argparse(request)
    return json.dumps(index.interp_latlon(**args))


@app.route('/index/interpolate/timestamp')
def interp_timestamp():
    """
    Return a set of parameter values interpolated between layers and
    points closest to the specified timestamp, latitude and longitude.
    The set of parameters values to return can be filtered.
    ---
    produces:
    - "application/json"
    parameters:
    - name: lat
      in: query
      description: Latitude for the interpolation point
      type: number
      required: true
    - name: lon
      in: query
      description: Longitude for the interpolation point
      type: number
      required: true
    - name: timestamp
      in: query
      description: Timestamp for the interpolation point
      type: string
      format: "Date time: %Y-%m-%dT%H:%M:%S.%fZ"
      required: true
    - name: parameter_name
      in: query
      description: Parameter name for filtering on layers containing only a certain parameter value such as "Wind speed"
      type: string
    - name: parameter_unit
      in: query
      description: Parameter unit name for filtering on layers containing only parameter values in a certain unit, such as m/s
      type: string
    - name: type_of_level
      in: query
      description: Type of level for filtering on only layers with a specified level of this type, such as "Meters above sea level"
      type: string
    - name: level
      in: query
      description: Level for filtering on only layers at this level. Combine with type_of_level to specify layers at e.g. 10m above sea level.
      type: number
    - name: level_highest_below
      in: query
      description: Find the layer at the highest level under the specified level (1) or at the lowest level above that level (0)
      type: integer
      default: 1
    - name: pretty
      in: query
      description: Pretty-print a single json object (true) or return newline separated json
      type: string
      enum:
        - true
    responses:
      200:
        description: "A set of parameter values"
    """
    args = argparse(request)
    pretty = args.pop("pretty", False)
    return format_result(index.interp_timestamp(**args), pretty)

@app.route('/index/add', methods=["POST"])
def add_file():
    """
    Add a new gribfile to the index
    ---
    consumes:
    - application/wmo-grib
    produces:
    - "application/json"
    parameters:
    - name: parametermap
      in: query
      description: Name of parametermap to use to translate parameter names in the file
      type: string
    - name: extra
      in: query
      description: Any extra data to add to the elasticsearch document
      type: string
      format: json
    - name: gribfile
      in: body
      description: The gribfile to add to the index
      schema:
        type: string
    responses:
      200:
        description: "The file was successfully added to the index"
    """
    args = argparse(request)
    filename = os.path.join(filearea, "%s.grb" % (uuid.uuid4(),))
    with open(filename, "wb") as f:
        f.write(request.get_data())
    index.add_file(filename, **args)
    return json.dumps({"status": "success"})

@app.route('/index/parametermap/add', methods=["POST"])
def parametermap_add_file():
    """
    Add a new parametermap to the index
    ---
    consumes:
    - text/csv
    produces:
    - "application/json"
    parameters:
    - name: name
      in: query
      description: Name of the new parametermap (to be used for the parametermap parameter of /index/add)
      type: string
      required: true
    - name: parametermap
      in: body
      description: The parametermap file to add. Should have the columns parameter,name,unit
      schema:
        type: string
        example: |
          parameter,name,unit
          "1","Pressure","Pa"
          "4","Potential vorticity","K m2 kg-1 s-1"
    responses:
      200:
        description: "The file was successfully added to the index"
    """
    args = argparse(request)
    filename = os.path.join(filearea, "%s.csv" % (uuid.uuid4(),))
    with open(filename, "wb") as f:
        f.write(request.get_data())
    index.add_parametermap(mapping=filename, **args)
    return json.dumps({"status": "success"})
    
@app.route('/index/parametermap')
def parametermap_list():
    """
    List available parametermaps
    ---
    produces:
    - "application/json"
    responses:
      200:
        description: "A list of parametermaps"
    """
    return json.dumps(index.get_parametermaps())

if __name__ == "__main__":
    app.run()
