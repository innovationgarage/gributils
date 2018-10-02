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
                        parametermap[str(row["parameter"])] = (row["name"], row["unit"])
                return parametermap
        return {}
                    
    def add_file(self, filepath):
        parametermap = self.load_parametermap(filepath)

        self.cur.execute("SELECT count(*) FROM gribfiles WHERE file = %s",
                     (filepath,))
        if self.cur.fetchone()[0] > 0:
            print("%s IGNORE" % filepath)
            return
        print("%s INDEX" % filepath)
        self.cur.execute("INSERT INTO gribfiles (file) VALUES (%s)",
                     (filepath,))
        with pygrib.open(filepath) as grbs:
            for grb_idx, grb in enumerate(grbs):
                # layer indexes start at 1
                layer_idx = grb_idx + 1
                parameter_name = grb.parameterName
                parameter_unit = grb.parameterUnits
                
                if "parameterNumber" in grb.keys():
                    if str(grb.parameterNumber) in parametermap:
                        parameter_name, parameter_unit = parametermap[str(grb.parameterNumber)]
                    elif "parameterCategory" in grb.keys():
                        parameter_code = "{}.{}".format(grb['parameterCategory'], grb['parameterNumber'])
                    if parameter_code in parametermap:
                        parameter_name, parameter_unit = parametermap[parameter_code]
                elif grb.parameterName in parametermap:
                    parameter_name, parameter_unit = parametermap[grb.parameterName]
                
                measurementid = "%s,%s,%s,%s" % (parameter_name, parameter_unit, grb.typeOfLevel, grb.level)
                gridid, poly = self.extract_polygons(grb)

                self.cur.execute("INSERT INTO gridareas (gridid, projparams, the_geom) VALUES (%s, %s, st_geomfromtext(%s, 4326)) ON CONFLICT DO NOTHING",
                            (gridid, json.dumps(grb.projparams), poly.wkt))

                self.cur.execute("""INSERT
                                 INTO measurement (measurementid, parameterName, parameterUnit, typeOfLevel, level)
                                 VALUES (%s, %s, %s, %s, %s)
                                 ON CONFLICT DO NOTHING""",
                            (measurementid, parameter_name, parameter_unit, grb.typeOfLevel, grb.level))

                self.cur.execute("INSERT INTO griblayers (file, layeridx, measurementid, timestamp, gridid) VALUES (%s, %s, %s, %s, %s)",
                            (filepath, layer_idx, measurementid, grb.validDate, gridid))

        self.cur.execute("COMMIT")

    def lookup(self, output="layers",
               lat=None, lon=None, timestamp=None, parameter_name=None, parameter_unit=None, type_of_level=None, level=None,
               timestamp_last_before=True, level_highest_below=True):
        """Return a set of griblayers matching the specified requirements"""

        args = dict(lat=lat, lon=lon, timestamp=timestamp,
                    parameter_name=parameter_name, parameter_unit=parameter_unit,
                    type_of_level=type_of_level, level=level)
        filters = []
        if lat is not None and lon is not None:
            filters.append("st_contains(gridareas.the_geom, ST_SetSRID(ST_Point(%(lon)s, %(lat)s), 4326))")
        if timestamp is not None:
            if timestamp_last_before:
                filters.append("""
                  griblayers.timestamp < %(timestamp)s
                  and (select count(*)
                       from griblayers as g2
                       where
                         g2.measurementid = gridlayers.measurementid
                         and g2.gridid = gridlayers.gridid
                         and g2.timestamp > gridlayers.timestamp
                      ) = 0
                """)
            else:
                pass
        if parameter_name is not None:
            filters.append("measurement.parameterName = %(parameter_name)s")
        if parameter_unit is not None:
            filters.append("measurement.parameterUnit = %(parameter_unit)s")
        if type_of_level is not None:
            filters.append("measurement.typeOfLevel = %(type_of_level)s")
        if level is not None:
            if level_highest_below:
                filters.append("""
                  measurement.level < %(level)s
                  and (select count(*)
                       from measurement as m2
                       where
                         m2.parameterName = measurement.parameterName
                         and m2.parameterUnit = measurement.parameterUnit
                         and m2.typeOfLevel = measurement.typeOfLevel
                         and m2.level > measurement.level
                      ) = 0
                """)
            else:
                pass

        sql = """
          from
            griblayers
            join measurement on 
              griblayers.measurementid = measurement.measurementid
            join gridareas on
              griblayers.gridid = gridareas.gridid
        """

        if filters:
            sql += " where " + "\n and ".join(filters)

        if output == "layers":
            sql = """
              select
                griblayers.file,
                griblayers.layeridx,
                griblayers.timestamp
              %s
              group by
                griblayers.file,
                griblayers.layeridx,
                griblayers.timestamp
            """ % sql
        elif output == "names":
            sql = """
              select
                measurement.parameterName
              %s
              group by
                measurement.parameterName
            """ % sql
        elif output == "units":
            sql = """
              select
                measurement.parameterUnit
              %s
              group by
                measurement.parameterUnit
            """ % sql
        elif output == "level-types":
            sql = """
              select
                measurement.typeOfLevel
              %s
              group by
                measurement.typeOfLevel
            """ % sql
        elif output == "levels":
            sql = """
              select
                measurement.level
              %s
              group by
                measurement.level
            """ % sql
        else:
            raise Exception("Unknown output. Available outputs are layers, names, units, level-types, levels")
            
        self.cur.execute(sql, args)
        return self.cur
