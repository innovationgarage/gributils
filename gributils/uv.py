import gributils.projection
import pyproj
import numpy as np

wgs84_geod = pyproj.Geod(ellps='WGS84')

def uv_to_magnitude_azimuth(grbU, grbV):
    proj = gributils.projection.LayerProjection(grbU)

    lats1, lons1 = grbU.latlons()
    x1, y1 = proj.project(lons1, lats1)

    x2 = x1 + grbU.values * proj.dx
    y2 = y1 + grbV.values * proj.dy

    lons2, lats2 = proj.unproject(x2, y2)
    azimuth, back, dist = wgs84_geod.inv(lons1, lats1, lons2, lats2)

    magnitude = np.sqrt(grbU.values**2 + grbV.values**2)
    return magnitude, azimuth
