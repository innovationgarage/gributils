import pygrib
import shapely.geometry
import shapely.ops
import shapely.affinity
import scipy.ndimage.morphology
import skimage.measure
import numpy as np
import pyproj
import functools
import hashlib

def bounds(layer, fill_holes=True, simplify=0.01):
    """Extracts a shapely.geometry.MultiPolygon object representing all
    areas with valid values in a grib file layer. Valid values are
    defined as grid cells with a value >= layer.minimum and <=
    layer.maximum.

    By default, 'holes' with invalid values completely surrounded by
    valid values are ignored. To exclude these areas from the
    generated polygons, set fill_holes=false.

    By default, the returned polygon has been smothed with a smothing
    factor of 0.01. To not smoth the polygon, set simplify=False. Note
    that unsmothed polygons will generally be very large and therefore
    slow to plot and doing point-in-polygon tests on.
    """
    
    validmap = (layer.values >= layer.minimum) & (layer.values <= layer.maximum)

    if fill_holes:
        validmap = scipy.ndimage.morphology.binary_fill_holes(validmap)
    
    framedvalidmap = np.zeros((validmap.shape[0] + 2, validmap.shape[1]+2))
    framedvalidmap[1:-1, 1:-1] = validmap
    contours = skimage.measure.find_contours(framedvalidmap, 0.5)

    def unframe(x, y):
        return x-1, y-1        
    
    validshape = shapely.geometry.MultiPolygon([(np.concatenate((cnt[:,1:], cnt[:,:1]), axis=1), []) for cnt in contours])
    validshape = shapely.ops.transform(unframe, validshape)

    proj = LayerProjection(layer)
    validshape = shapely.ops.transform(proj.scale, validshape)
    validshape = shapely.ops.transform(proj.unproject, validshape)
    
    validshape = split_dateline(validshape)
    validshape = unwrap_dateline(validshape)

    if simplify is not False:
        validshape = validshape.simplify(simplify)

    return validshape

def polygon_id(polygon):
    """Returns a hash value of a polygon/multipolygon"""
    return hashlib.sha256(polygon.wkb).hexdigest()

class LayerProjection(object):
    def __init__(self, layer):
        self.layer = layer
        self.projparams = layer.projparams
        if self.projparams["proj"] == "cyl":
            self.projparams = {"init": 'epsg:4326'}
            if "Nx" in layer.keys():
                self.nx = layer.Nx
            else:
                self.nx = layer.Ni
            if "Ny" in layer.keys():
                self.ny = layer.Ny
            else:
                self.ny = layer.Nj
            self.dy = (layer.latitudeOfLastGridPointInDegrees - layer.latitudeOfFirstGridPointInDegrees) / (self.ny - 1)
            self.dx = (layer.longitudeOfLastGridPointInDegrees - layer.longitudeOfFirstGridPointInDegrees) / (self.nx  - 1)
        else:
            if "DxInMetres" in layer.keys():
                self.dx = layer.DxInMetres
            else:
                self.dx = layer.DiInMetres
            if "DyInMetres" in layer.keys():
                self.dy = layer.DyInMetres
            else:
                self.dy = layer.DjInMetres

        self.gridproj = pyproj.Proj(**self.projparams)
        self.gridproj_over = pyproj.Proj(over=True, **self.projparams)
        self.wgs84 = pyproj.Proj(over=True, init='epsg:4326')

        self.project = functools.partial(pyproj.transform, self.wgs84, self.gridproj)
        self.unproject = functools.partial(pyproj.transform, self.gridproj_over, self.wgs84)

        self.x0, self.y0 = self.project((layer.longitudeOfFirstGridPointInDegrees + 180) % 360 - 180,
                                        layer.latitudeOfFirstGridPointInDegrees)

    def scale(self, x, y):
        """Returns x,y in projected units (suitable for
        self.unproject) given input in grid coordinates x_index, y_index"""
        return self.x0 + self.dx*x, self.y0 + self.dy*y

def unwrap_dateline(multipolygon):
    """Move polygons in a multipolygon inside longitude ]-180,180[,
    assuming that they do not span the dateline."""
    
    geoms = []
    for geom in multipolygon.geoms:
        maxlon = max(lon for (lon, lat) in geom.exterior.coords)
        offset = -((maxlon+180)//360)*360
        if (maxlon + 180) % 360 == 0:
            offset += 360
        geoms.append(shapely.affinity.translate(geom, offset))
    return shapely.geometry.MultiPolygon(geoms)

def split_dateline(multipolygon):
    for pos in (-540, -180, 180, 540):
        res = shapely.ops.split(multipolygon, shapely.geometry.LineString([(pos, 90), (pos, -90)]))
        if res.geoms:
            multipolygon = shapely.geometry.MultiPolygon(res)
    return multipolygon
