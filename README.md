# gributils
Utils on top of pygrib for extracting metadata and manipulating grib files.

# gributils
gributils is a set of utilities on top of pygrib for manipulating and
indexing gribfiles. Current features include:

* Grib index
  * Store and query a large set (historical dataset) of grib files
  * PostGIS based
  * Query by geographical location,, timestamp and parameter name,
    parameter unit, level and level type.
  * Parameter name normalization using a simple CSV file
* Extract polygons of areas covered with valid values in a grib layer
* Provide accurate grid coordinates, even for grib version 1 files
  where too low precision in Dx / Dy have resulted in a distorted grid.
* Conversion from values with U and V components (e.g. for wind) to
  magnitude and degree north.

