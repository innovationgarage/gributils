import os
import sys
import pygrib
import psycopg2
import shapely.geometry
import shapely.ops
import shapely.affinity
import scipy.ndimage.morphology
from scipy import interpolate
import skimage.measure
import numpy as np
import json
import pyproj
import functools
import hashlib
import gributils.bounds
import csv
from datetime import datetime

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
                    
    def add_file(self, filepath, threshold=0.5):
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

                # ##HERE!! cartodb_id is not set yet, what should I do?! Stalled for now FIXME later
                # self.cur.execute("""SELECT
                #                       a.cartodb_id, b.cartodb_id, ST_HausdorffDistance(a.the_geom, b.the_geom)
                #                     FROM
                #                       gridareas AS a, gridareas AS b
                #                     WHERE
                #                       a.cartodb_id = VALUES (%s)
                #                       and b.cartodb_id <> a.cartodb_id
                #                       and ST_HausdorffDistance(a.the_geom, b.the_geom) < VALUES (%s)
                #                       and a.is_reference IS NULL
                #                       and (SELECT count(*) 
                #                            FROM gridareas AS c
                #                            WHERE
                #                              ST_HausdorffDistance(a.the_geom, c.the_geom)<ST_HausdorffDistance(a.the_geom, b.the_geom)
                #                              and c.cartodb_id <> b.cartodb_id
                #                              and c.cartodb_id <> a.cartodb_id
                #                           )=0""",
                #                  (cartodb_id, threshold))
                
                self.cur.execute("""INSERT
                                 INTO measurement (measurementid, parameterName, parameterUnit, typeOfLevel, level)
                                 VALUES (%s, %s, %s, %s, %s)
                                 ON CONFLICT DO NOTHING""",
                            (measurementid, parameter_name, parameter_unit, grb.typeOfLevel, grb.level))

                self.cur.execute("INSERT INTO griblayers (file, layeridx, measurementid, analdate, validdate, gridid) VALUES (%s, %s, %s, %s, %s, %s)",
                            (filepath, layer_idx, measurementid, grb.analDate, grb.validDate, gridid))

        self.cur.execute("COMMIT")

    def lookup(self, output="layers",
               lat=None, lon=None, timestamp=None, parameter_name=None, parameter_unit=None, type_of_level=None, level=None,
               timestamp_last_before=1, level_highest_below=True):
        """Return a set of griblayers matching the specified requirements"""
        
        args = dict(lat=lat, lon=lon, timestamp=timestamp,
                    parameter_name=parameter_name, parameter_unit=parameter_unit,
                    type_of_level=type_of_level, level=level)
        filters = []
        if lat is not None and lon is not None:
            filters.append("gridareas.is_reference='t' and st_contains(gridareas.the_geom, ST_SetSRID(ST_Point(%(lon)s, %(lat)s), 4326))")
        if timestamp is not None:
            if timestamp_last_before==1:
                filters.append("""
                  griblayers.validdate <= %(timestamp)s""")
                #   and (select count(*)
                #        from griblayers as g2
                #        where
                #          g2.measurementid = griblayers.measurementid
                #          and g2.gridid = griblayers.gridid
                #          and g2.validdate > griblayers.validdate
                #          and g2.validdate <= %(timestamp)s
                #       ) = 0
                # """)
            else:
                filters.append("""
                  griblayers.validdate >= %(timestamp)s""")
                #   and (select count(*)
                #        from griblayers as g2
                #        where
                #          g2.measurementid = griblayers.measurementid
                #          and g2.gridid = griblayers.gridid
                #          and g2.validdate < griblayers.validdate
                #          and g2.validdate >= %(timestamp)s
                #       ) = 0
                # """)
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
                griblayers.analdate,
                griblayers.validdate
              %s
              group by
                griblayers.file,
                griblayers.layeridx,
                griblayers.analdate,
                griblayers.validdate
             order by
                griblayers.validdate
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

        try:
            self.cur.execute(sql, args)
            if timestamp_last_before==1:
                res = self.cur.fetchall()
                if res:
                    return res[-1]
                else:
                    return None
            else:
                return self.cur.fetchone()
        except:
            return None
#        return self.cur

    def interp(self, layer, point):
        data = layer.data()
        x = data[2][0,:]
        y = data[1][:,0]
        z = data[0]
        f = interpolate.interp2d(x, y, z, kind='cubic')        
        xnew, ynew = point
        return f(xnew, ynew)
    
    def interp_latlon(self,
                     gribfile=None, layeridx=None,
                     lat=None, lon=None):

        try:
            layer = pygrib.open(gribfile)[int(layeridx)]
            new_value = self.interp(layer, (lat, lon))
            return new_value
        except:
            print('No such file!')
            return None

    def interp_timestamp(self, lat=None, lon=None, timestamp=None,
                         parameter_name=None, parameter_unit=None,
                         type_of_level=None, level=None,
                         timestamp_last_before=1, level_highest_below=True):

        #FIXME! properly handle the scenario with more (or less) than one hit for eaither lst_before or first_after layers
        layer_last_before =  self.lookup(output="layers",
                                         lat=lat, lon=lon, timestamp=timestamp,
                                         parameter_name=parameter_name, parameter_unit=parameter_unit,
                                         type_of_level=type_of_level, level=level,
                                         timestamp_last_before=1, level_highest_below=True)
        
        layer_first_after =  self.lookup(output="layers",
                                         lat=lat, lon=lon, timestamp=timestamp,
                                         parameter_name=parameter_name, parameter_unit=parameter_unit,
                                         type_of_level=type_of_level, level=level,
                                         timestamp_last_before=0, level_highest_below=True)

        if layer_last_before and layer_first_after:
            data_last_before = pygrib.open(layer_last_before[0])[int(layer_last_before[1])]
            data_first_after = pygrib.open(layer_first_after[0])[int(layer_first_after[1])]
            
            timestamp_last_before = int(layer_last_before[3].strftime("%s"))
            timestamp_first_after = int(layer_first_after[3].strftime("%s"))
            
            parameter_last_before = self.interp(data_last_before, (lat, lon))[0]
            parameter_first_after = self.interp(data_first_after, (lat, lon))[0]
            
            x = np.array([timestamp_last_before, timestamp_first_after])
            y = np.array([parameter_last_before, parameter_first_after])
            f = interpolate.interp1d(x, y)
        
            if isinstance(timestamp, str):
                try:
                    ts = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
                except:
                    ts = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")
            else:
                ts = timestamp
                
            return f(int(ts.strftime("%s")))            
        else:
            return None
            

        
