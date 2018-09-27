import os
import sys
import pygrib
import psycopg2
import shapely.geometry
import shapely.ops
import shapely.affinity
import scipy.ndimage.morphology
import skimage.measure
import numpy as np
import json
import pyproj
import functools
import hashlib
import gributils.bounds
import csv

class GribIndex(object):
    def __init__(self, db_connect_string):
        self.conn = psycopg2.connect(db_connect_string)
        self.cur = self.conn.cursor()

    def extract_polygons(self, layer):
        shape = gributils.bounds.bounds(layer)
        return gributils.bounds.polygon_id(shape), shape

    def add_dir(self, basedir, cb):
        for root, dirs, files in os.walk(basedir):
            for filename in files:
                if not (filename.endswith(".grib") or filename.endswith(".grb")): continue
                filepath = os.path.abspath(os.path.join(root, filename))
                try:
                    self.add_file(filepath)
                except Exception as e:
                    cb({
                        "file": filepath,
                        "error": e
                        })

    def load_parametermap(self, filepath):
        for parametermap_file in (filepath + ".parametermap.csv",
                                  os.path.join(os.path.dirname(filepath), "parametermap.csv")):
            if os.path.exists(parametermap_file):
                parametermap = {}
                with open(parametermap_file) as f:
                    for row in csv.DictReader(f):
                        parametermap[int(row["parameter"])] = (row["name"], row["unit"])
                return parametermap
        return {}
                    
    def add_file(self, filepath):
        parametermap = self.load_parametermap(filepath)
        
        self.cur.execute("SELECT count(*) FROM gribfiles WHERE file = %s",
                     (filepath,))
        if self.cur.fetchone()[0] > 0:
            print("%s IGNORE" % filepath)
            continue
        print("%s INDEX" % filepath)
        self.cur.execute("INSERT INTO gribfiles (file) VALUES (%s)",
                     (filepath,))
        with pygrib.open(filepath) as grbs:
            for grb in grbs:
                parameter_name = grb.parameterName
                parameter_unit = grb.parameterUnits
                if grb.parameterNumber in parametermap:
                    parameter_name, parameter_unit = parametermap[grb.parameterNumber]
              
                measurementid = "%s,%s,%s" % (parameter_name, parameter_unit, grb.typeOfLevel, grb.level)
                gridid, poly = extract_polygons(grb)

                self.cur.execute("INSERT INTO gridareas (gridid, projparams, the_geom) VALUES (%s, %s, st_geomfromtext(%s, 4326)) ON CONFLICT DO NOTHING",
                            (gridid, json.dumps(grb.projparams), poly.wkt))

                self.cur.execute("""INSERT
                                 INTO measurement (measurementid, parameterName, parameterUnit, typeOfLevel, level)
                                 VALUES (%s, %s, %s, %s)
                                 ON CONFLICT DO NOTHING""",
                            (measurementid, parameter_name, parameter_unit, grb.typeOfLevel, grb.level))

                self.cur.execute("INSERT INTO griblayers (file, measurementid, timestamp, gridid) VALUES (%s, %s, %s, %s)",
                            (filepath, measurementid, grb.validDate, gridid))

        self.cur.execute("COMMIT")

    def lookup(self, lat=None, lon=None, timestamp=None, parameterName=None, parameterUnit=None, typeOfLevel=None, level=None, last_before=True):
        """Return a set of griblayers matching the specified requirements"""

        self.cur.execute("""
          select
            griblayers.*
          from
            griblayers
            join measurement on 
              griblayers.measurementid = measurement.measurementid
            join gridareas on
              griblayers.gridid = gridareas.gridid
          where



        """ % {})
