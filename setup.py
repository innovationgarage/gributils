#!/usr/bin/env python

import setuptools

setuptools.setup(name='gributils',
      version='0.1',
      description='Utils on top of pygrib for extracting metadata and manipulating grib files',
      author='Egil Moeller',
      author_email='egil@innovationgarage.no',
      url='https://github.com/innovationgarage/gributils',
      packages=setuptools.find_packages(),
      install_requires=[
          'numpy',
          'pyproj',
          'pygrib==2.0.2',
          'shapely',
          'scipy',
          'scikit-image',
          'click'
      ],
      include_package_data=True,
      entry_points='''
      [console_scripts]
      gributils = gributils.cli:main
      '''
  )
