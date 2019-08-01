import os
import sys
import pygrib
import shapely.geometry
import shapely.ops
import shapely.affinity
import shapely.wkt
import scipy.ndimage.morphology
from scipy import interpolate
import skimage.measure
import numpy as np
import json
import pyproj
import functools
import hashlib
import gributils.bounds
import gributils.layer
import csv
from datetime import datetime
import requests

def check_result(res):
    try:
        res.raise_for_status()
        return res
    except Exception as e:
        raise Exception("%s: %s" % (e, res.content))

def check_es_result(res):
    res = check_result(res)
    if not res.json().get('acknowledged'):
        raise Exception(json.dumps(res.json(), indent=2))
    return res
     
class GribIndex(object):
    def __init__(self, es_url):
        self.es_url = es_url
        self.gridcache = set()
        self.parametermapcache = {}
        self.gribcache = gributils.layer.GribCache()
        self.layercache = gributils.layer.LayerCache()
        
    def extract_polygons(self, layer):
        shape = gributils.bounds.bounds(layer)
        return gributils.bounds.polygon_id(shape), shape

    def init_db(self):
        check_es_result(
            requests.put("%s/geocloud-gribfile-parametermap" % self.es_url,
                         json={
                             "mappings": {
                                 "doc": {
                                     "properties": {
                                         "name": {"type": "keyword"},
                                         "mapping": {"type": "object"}
                                     }
                                 }
                             }
                         }))
        
        check_es_result(
            requests.put("%s/geocloud-gribfile-grid" % self.es_url,
                         json={
                             "mappings": {
                                 "doc": {
                                     "properties": {
                                         "gridid": {"type": "keyword"},
                                         "projparams": {"type": "object"},
                                         "polygon": {
                                             "type": "geo_shape",
                                             "strategy": "recursive"
                                         }
                                     }
                                 }
                             }
                         }))

        check_es_result(
            requests.put("%s/geocloud-gribfile-layer" % self.es_url,
                         json={
                             "mappings": {
                                 "doc": {
                                     "properties": {
                                         "gridid": {"type": "keyword"},
                                         
                                         "parameterName": {"type": "keyword"},
                                         "parameterUnit": {"type": "keyword"},
                                         "typeOfLevel": {"type": "keyword"},
                                         "level": {"type": "double"},
                                         
                                         "validDate": {"type": "date"},
                                         "analDate": {"type": "date"},
                                         
                                         "url": {"type": "keyword"},
                                         "idx": {"type": "integer"}                                      
                                     }
                                 }
                             }
                         }))

    def add_parametermap(self, name, mapping):
        parametermap = {}
        with open(mapping) as f:
            for row in csv.DictReader(f):
                parametermap[str(row["parameter"])] = (row["name"], row["unit"])
        
        check_result(
            requests.post("%s/geocloud-gribfile-parametermap/doc" % self.es_url, json = {
                "name": name,
                "mapping": parametermap}))
            
    def get_parametermaps(self):
        res = check_result(
            requests.post("%s/geocloud-gribfile-parametermap/_search" % self.es_url,
                          json={
                              "_source": ["name"],
                              "query":{
                                  "bool": {
                                      "must": {
                                          "match_all": {}
                                      }
                                  }
                              }
                          }))
        return [item["_source"]["name"] for item in res.json()["hits"]["hits"]]
        
    def get_grids_for_position(self, lat, lon):
        res = check_result(
            requests.post("%s/geocloud-gribfile-grid/_search" % self.es_url,
                          json={
                              "_source": ["gridid"],
                              "query":{
                                  "bool": {
                                      "must": {
                                          "match_all": {}
                                      },
                                      "filter": {
                                          "geo_shape": {
                                              "polygon": {
                                                  "shape": {
                                                      "type": "point",
                                                      "coordinates": [lon, lat]
                                                  },
                                                  "relation": "contains"
                                              }
                                          }
                                      }
                                  }
                              }
                          }))
        return [item["_source"]["gridid"] for item in res.json()["hits"]["hits"]]
        
    def get_grid_for_layer(self, grb):
        gridid, poly = self.extract_polygons(grb)
        if gridid in self.gridcache:
            return gridid
        print("Cache miss for", gridid)

        res = check_result(
            requests.post("%s/geocloud-gribfile-grid/_search" % self.es_url,
                          json={"query": {"bool": {"must": {"match": {"gridid": gridid}}}}}))

        if res.json()["hits"]["total"] == 0:
            print("INSERT NEW GRID", repr({
                "gridid": gridid,
                "projparams": grb.projparams,
                "polygon": poly.wkt}))
            res = check_result(
                requests.post("%s/geocloud-gribfile-grid/doc" % self.es_url, json = {
                    "gridid": gridid,
                    "projparams": grb.projparams,
                    "polygon": poly.wkt}))

        self.gridcache.add(gridid)
        
        return gridid

    def get_grid_bboxes(self):
        res = check_result(
            requests.post("%s/geocloud-gribfile-grid/_search" % self.es_url,
                          json={
                              "query": {"match_all": {}},
                              "size": 10000
                          }))
        return {hit["_source"]["gridid"]: shapely.wkt.loads(hit["_source"]["polygon"]).bounds
                for hit in res.json()["hits"]["hits"]}
    
    def map_parameter(self, filepath, grb, **kw):
        parametermap = self.load_parametermap(filepath, **kw)
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

        return parameter_name, parameter_unit

    def load_parametermap(self, filepath, parametermap=None, **kw):
        if parametermap is None:
            parametermap = os.path.basename(os.path.dirname(filepath))

        if parametermap not in self.parametermapcache:
            res = check_result(
                requests.post("%s/geocloud-gribfile-parametermap/_search" % self.es_url,
                              json={"query":{"bool": {"must": {"term": {"name": parametermap}}}}}))
            res = res.json()["hits"]["hits"]
            if len(res):
                self.parametermapcache[parametermap] = res[0]["_source"]["mapping"]                
            else:
                self.parametermapcache[parametermap] = {}
                
        return self.parametermapcache[parametermap]
    
    def add_layer(self, grb, url, idx, **kw):
        check_result(
            requests.post("%s/geocloud-gribfile-layer/doc" % self.es_url,
                          json = self.format_layer(grb, url, idx, **kw)))

    def format_layer(self, grb, url, idx, extra={}, **kw):
        gridid = self.get_grid_for_layer(grb)

        parameter_name, parameter_unit = self.map_parameter(url, grb, **kw)

        res = {
            "gridid": gridid,

            "parameterName": parameter_name,
            "parameterUnit": parameter_unit,
            "typeOfLevel": grb.typeOfLevel,
            "level": grb.level,

            "validDate": grb.validDate.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "analDate": grb.analDate.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),

            "url": url,
            "idx": idx
        }
        res.update(extra)
        return res
        
    
    def add_file(self, filepath, **kw):
        print("Adding file", filepath)
        with pygrib.open(filepath) as grbs:
            layers = [self.format_layer(grb, filepath, grb_idx+1, **kw)
                      for grb_idx, grb in enumerate(grbs)]
        data = "".join(
            json.dumps({"index": {"_index": "geocloud-gribfile-layer", "_type":"doc"}}) + "\n" +
            json.dumps(layer) + "\n"
            for layer in layers)
        res = check_result(
            requests.post("%s/_bulk" % self.es_url,
                          data = data,
                          headers = {'Content-Type': 'application/json'}))
        assert not res.json()["errors"], repr(res.json())
            
    def add_dir(self, basedir, cb, **kw):
        for root, dirs, files in os.walk(basedir):
            for filename in files:
                if not (filename.endswith(".grib") or filename.endswith(".grb")): continue
                filepath = os.path.abspath(os.path.join(root, filename))
                try:
                    self.add_file(filepath, **kw)
                except Exception as e:
                    cb({
                        "file": filepath,
                        "error": e
                        })

    def lookup(self, output="layers",
               lat=None, lon=None, timestamp=None, parameter_name=None, parameter_unit=None, type_of_level=None, level=None,
               timestamp_last_before=1, level_highest_below=True):
        """Return a set of griblayers matching the specified requirements"""
        
        aggregation = None
        
        if output == "layers":
            pass
        elif output == "names":
            aggregation = {"terms": {"field": "parameterName"}}
        elif output == "units":
            aggregation = {"terms": {"field": "parameterUnit"}}
        elif output == "level-types":
            aggregation = {"terms": {"field": "typeOfLevel"}}
        elif output == "levels":
            aggregation = {"terms": {"field": "level"}}
        else:
            raise Exception("Unknown output. Available outputs are layers, names, units, level-types, levels")

        if lat is not None:
            assert lon is not None, "lat and lon must both be set, or must both be left unset"
            
            gridids = self.get_grids_for_position(lat, lon)

        filters = []
        if lat is not None and lon is not None:
            filters.append({
                "terms": {
                    "gridid": gridids
                }
            })

        if timestamp is not None:
            filters.append({
                "range" : {
                    "validDate" : {
                        ["gte", "lte"][not not timestamp_last_before]: timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                    }
                }
            })
            if aggregation is None:
                aggregation = {
                    "terms": {
                        "script" : {
                            "source": "doc.parameterName + \"-\" + doc.parameterUnit + \"-\" + doc.typeOfLevel + \"-\" + doc.level",
                            "lang": "painless"
                        },
                        "size": 100000
                    },
                    "aggs": {
                        "results": {
                            "top_hits": {
                                "sort": [
                                    {"validDate": {"order": ["asc", "desc"][not not timestamp_last_before]}}
                                ],
                                "size" : 1
                            }
                        }
                    }
                }
        if parameter_name is not None:
            filters.append({"term": {"parameterName": parameter_name}})
        if parameter_unit is not None:
            filters.append({"term": {"parameterUnit": parameter_unit}})
        if type_of_level is not None:
            filters.append({"term": {"typeOfLevel": type_of_level}})
        if level is not None:
            filters.append({
                "range": {
                    "level": {
                        ["gte", "lte"][not not level_highest_below]: level
                    }
                }
            })            

        if not filters:
            filters = {"match_all": {}}

        if aggregation:
            query = {
                "aggs" : {
                    "results": {
                        "filter": {"bool": {"must": filters}},
                        "aggs": {
                            "results": aggregation
                        }
                    }
                },
                "size": 0
            }
        else:
            query = {
                "query": {
                    "bool": {"must": filters}
                },
                "size": 10000
            }

        #print(json.dumps(query, indent=2))
            
        res = check_result(
            requests.post("%s/geocloud-gribfile-layer/_search" % self.es_url,
                          json=query))
        res = res.json()

        if aggregation is not None:
            res = res["aggregations"]["results"]["results"]["buckets"]
            if res and "results" in res[0]:
                res = [subentry["_source"]
                       for entry in res
                       for subentry in entry["results"]["hits"]["hits"]]
        else:
            res = [item["_source"] for item in res["hits"]["hits"]]
        return res
    
    def interp_latlon(self,
                     gribfile=None, layeridx=None,
                     lat=None, lon=None):

        try:
            layer = self.layercache.get(gribfile, int(layeridx))
            return layer.interpolate(lat, lon)[0]
        except Exception as e:
            print('Unable to load layer:', e)
            return None

    def interp_timestamp(self, lat=None, lon=None, timestamp=None,
                         parameter_name=None, parameter_unit=None,
                         type_of_level=None, level=None,
                         level_highest_below=True):

        # FIXME: Interpolate along levels too maybe?

        if isinstance(timestamp, str):
            try:
                timestamp = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
            except:
                timestamp = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")
        timestamp_int = int(timestamp.strftime("%s"))

        def synthesize_uv_entries(entries):
            components = {"U": {}, "V": {}}
            for entry in entries:
                if " component of " in entry["parameterName"]:
                    compname, name = entry["parameterName"].split(" component of ")
                    components[compname][(name, entry["parameterUnit"], entry["typeOfLevel"], entry["level"], entry["validDate"])] = entry
                yield entry
            for key, entryU in components["U"].items():
                entryV = components["V"][key]
                entry = dict(entryU)
                entry["idx"] = (entryU["idx"], entryV["idx"], "azimuth")
                entry["parameterName"] = "Azimuth component of %s" % key[0]
                yield entry
                entry = dict(entryU)
                entry["idx"] = (entryU["idx"], entryV["idx"], "magnitude")
                entry["parameterName"] = "Magnitude component of %s" % key[0]
                yield entry
                
        def to_map(entries):
            return {
                (entry["parameterName"], entry["parameterUnit"], entry["typeOfLevel"], entry["level"]):
                self.layercache.get(entry["url"], entry["idx"])
                for entry in entries}

        def interpolate_parameter(data_last_before, data_first_after):
            parameter_last_before = data_last_before.interpolate(lat, lon)[0]
            parameter_first_after = data_first_after.interpolate(lat, lon)[0]

            timestamp_last_before = data_last_before.valid_date
            timestamp_first_after = data_first_after.valid_date

            if timestamp_last_before == timestamp_first_after:
                # Avoid a divide by zero in interp1d...
                return parameter_last_before
            
            x = np.array([timestamp_last_before, timestamp_first_after])
            y = np.array([parameter_last_before, parameter_first_after])
            
            f = interpolate.interp1d(x, y)
        
            return float(f(timestamp_int))
        
        layer_last_before =  to_map(
            synthesize_uv_entries(
                self.lookup(output="layers",
                            lat=lat, lon=lon, timestamp=timestamp,
                            parameter_name=parameter_name, parameter_unit=parameter_unit,
                            type_of_level=type_of_level, level=level,
                            timestamp_last_before=1, level_highest_below=level_highest_below)))
        
        layer_first_after = to_map(
            synthesize_uv_entries(
                self.lookup(output="layers",
                            lat=lat, lon=lon, timestamp=timestamp,
                            parameter_name=parameter_name, parameter_unit=parameter_unit,
                            type_of_level=type_of_level, level=level,
                            timestamp_last_before=0, level_highest_below=level_highest_below)))

        return [{"parameterName": key[0],
                 "parameterUnit": key[1],
                 "typeOfLevel": key[2],
                 "level": key[3],
                 "value": interpolate_parameter(layer_last_before[key], layer_first_after[key])}
                for key in layer_last_before.keys()
                if key in layer_first_after]
