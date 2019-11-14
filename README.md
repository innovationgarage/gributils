# gributils
gributils is a set of utilities on top of pygrib for manipulating and
indexing gribfiles. Current features include:

* Grib index
  * Store and query a large set (historical dataset) of grib files
  * Elastic Search
  * Query by geographical location, timestamp and parameter name,
    parameter unit, level and level type.
  * Parameter name normalization using a simple CSV file
  * Optional REST based API server
* Extract polygons of areas covered with valid values in a grib layer
* Provide accurate grid coordinates, even for grib version 1 files
  where too low precision in Dx / Dy have resulted in a distorted grid.
* Conversion from values with U and V components (e.g. for wind) to
  magnitude and degree north.


# Command line usage

Lookup parameter values for a certain point in space and time, across
multiple indexed grib files:

    ex@ample:~# gributils index --database="http://elasticsearch:9200" interp-timestamp --timestamp "2018-08-21 18:30:00" --lat 63 --lon 10
    [{'parameterName': 'P Pressure',          'parameterUnit': 'Pa',    'typeOfLevel': 'heightAboveGround', 'level': 0,  'value': 101633.03125000001},
     {'parameterName': 'U component of wind', 'parameterUnit': 'm s-1', 'typeOfLevel': 'heightAboveGround', 'level': 10, 'value': -2.898160457611084},
     {'parameterName': 'V component of wind', 'parameterUnit': 'm s-1', 'typeOfLevel': 'heightAboveGround', 'level': 10, 'value': 2.705005645751954}]

# Python usage

Lookup parameter values for a certain point in space and time, across
multiple indexed grib files:

    >>> import gributils.gribindex, datetime
    >>> index = gributils.gribindex.GribIndex("http://elasticsearch:9200")
    >>> index.interp_timestamp(lat=63, lon=10, timestamp=datetime.datetime(2018, 8, 21, 19, 32, 00))
    [{'parameterName': 'P Pressure',          'parameterUnit': 'Pa',    'typeOfLevel': 'heightAboveGround', 'level': 0,  'value': 101673.95000000001},
     {'parameterName': 'U component of wind', 'parameterUnit': 'm s-1', 'typeOfLevel': 'heightAboveGround', 'level': 10, 'value': -2.0344434102376305},
     {'parameterName': 'V component of wind', 'parameterUnit': 'm s-1', 'typeOfLevel': 'heightAboveGround', 'level': 10, 'value': 1.9993160883585617}]


Extracting layer boundaries from a grib layer:

    >>> import gributils.bounds, pygrib
    >>> polygon = gributils.bounds.bounds(pygrib.open("2018-08-20T14:27:30+00:00.mywavewam4_weatherapi_sorlandet.grb")[1])
    >>> polygon
    <shapely.geometry.polygon.Polygon object at 0x7fcd15abb208>

    >>> # poly_real_coords only has coordinates in the ranges ]-90,90[, ]-180,180[
    >>> poly_real_coords = gributils.bounds.unwrap_dateline(gributils.bounds.split_dateline(polygon))
    >>> poly_real_coords
    <shapely.geometry.multipolygon.MultiPolygon object at 0x7fcd0d4aab38>

    >>> # Calculate a unique id identifying this polygon that can be used for database lookups etc
    >>> gributils.bounds.polygon_id(poly_real_coords)
    '36cf52a2cd2ee4aed01095e7cd831f5f2c9a96c292eaca40de1b2d63c62ab1b8'

# REST usage

The rest API is described in the swagger documentation available at http://localhost:1028/

    ex@ample:~# gributils server --database="http://elasticsearch:9200"

    ex@ample:~# curl 'http://localhost:1028/index/interpolate/timestamp?lat=63&lon=10&timestamp=2018-08-21T19:32:00.000000Z'
    {'parameterName': 'P Pressure',          'parameterUnit': 'Pa',    'typeOfLevel': 'heightAboveGround', 'level': 0,  'value': 101673.95000000001}
    {'parameterName': 'U component of wind', 'parameterUnit': 'm s-1', 'typeOfLevel': 'heightAboveGround', 'level': 10, 'value': -2.0344434102376305}
    {'parameterName': 'V component of wind', 'parameterUnit': 'm s-1', 'typeOfLevel': 'heightAboveGround', 'level': 10, 'value': 1.9993160883585617}

# Installation

    apt install libgrib-api-dev libeccodes-dev
    pip install pyproj numpy flask
    python setup.py install

# Additional tools

[Gributils annotator](https://github.com/innovationgarage/gributils-annotator) lets you annotate streams of positional data with weather using gributils.
