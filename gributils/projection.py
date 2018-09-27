import pyproj
import functools

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
