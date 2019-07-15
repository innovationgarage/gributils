import pygrib
from scipy import interpolate
import numpy as np
import datetime
import gributils.uv

class GribCacheEntry(object):
    def __init__(self, filepath):
        self.filepath = filepath
        self.last_access = datetime.datetime.now()
        self.grbs = pygrib.open(filepath)
        
class GribCache(object):
    def __init__(self, size=10):
        self.size = size
        self.entries = {}

    def get(self, filepath):
        if filepath not in self.entries:
            if len(self.entries) >= self.size:
                entries = list(entries.values)
                entries.sort(key=lambda e: e.last_access)
                del self.entries[entries[0].filepath]
                entries[0].grbs.close()
            self.entries[filepath] = GribCacheEntry(filepath)
        entry = self.entries[filepath]
        entry.last_access = datetime.datetime.now()
        return entry.grbs

class Layer(object):
    def __init__(self, layers, idx):
        self.layers = layers
        self.idx = idx
        self.layer = layers[idx]

        data = self.layer.data()
        x = data[2][0,:]
        y = data[1][:,0]
        z = data[0]
        self.interpolate = interpolate.interp2d(x, y, z, kind='cubic')

        self.valid_date = int(self.layer.validDate.strftime("%s"))

class LayerUVComponent(object): pass

class LayerUV(object):
    def __init__(self, layers, idxU, idxV):
        self.layers = layers
        self.idxU = idxU
        self.idxV = idxV
        self.layerU = layers[idxU]
        self.layerV = layers[idxV]
                
        self.magnitude = LayerUVComponent()
        self.azimuth = LayerUVComponent()
        self.magnitude.data, self.azimuth.data = gributils.uv.uv_to_magnitude_azimuth(self.layerU, self.layerV)

        data, lats, lons = self.layerU.data()
        x = lons[0,:]
        y = lats[:,0]

        self.magnitude.interpolate = interpolate.interp2d(x, y, self.magnitude.data, kind='cubic')
        self.azimuth.interpolate = interpolate.interp2d(x, y, self.azimuth.data, kind='cubic')

        self.valid_date = int(self.layerU.validDate.strftime("%s"))
        self.magnitude.valid_date = self.valid_date
        self.azimuth.valid_date = self.valid_date
        
class LayerCacheEntry(object):
    def __init__(self, key, layer):
        self.key = key
        self.last_access = datetime.datetime.now()
        self.layer = layer

class LayerCache(object):
    def __init__(self, size=100, filessize=10):
        self.size = size
        self.entries = {}
        self.gribcache = GribCache(filessize)

    def get(self, filepath, idx):
        key = (filepath, idx)
        if key not in self.entries:
            if len(self.entries) >= self.size:
                entries = list(entries.values)
                entries.sort(key=lambda e: e.last_access)
                del self.entries[entries[0].key]
            file = self.gribcache.get(filepath)
            if isinstance(idx, tuple):
                if len(idx) == 2:
                    layer = LayerUV(file, idx[0], idx[1])
                else:
                    layer = getattr(self.get(filepath, idx[:2]), idx[2])
            else:
                layer = Layer(file, idx)
            self.entries[key] = LayerCacheEntry(key, layer)
        entry = self.entries[key]
        entry.last_access = datetime.datetime.now()
        return entry.layer
