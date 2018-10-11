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
import gributils.projection

def bounds(layer, fill_holes=True, simplify=0.01, add_buffer=0.3):
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

    proj = gributils.projection.LayerProjection(layer)
    validshape = shapely.ops.transform(proj.scale, validshape)
    validshape = shapely.ops.transform(proj.unproject, validshape)
    
    validshape = split_dateline(validshape)
    validshape = unwrap_dateline(validshape)

    if add_buffer:
        validshape = validshape.buffer(add_buffer)
    if simplify is not False:
        validshape = validshape.simplify(simplify)


    return validshape

def polygon_id(polygon):
    """Returns a hash value of a polygon/multipolygon"""
    return hashlib.sha256(polygon.wkb).hexdigest()


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
