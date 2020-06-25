#!/usr/bin/env python
import setuptools

from distutils.core import setup

setup(name='cds_downloader',
      version='0.1',
      description='Climate Data Store Downloader',
      license='MIT',
      author='Georg A. Seyerl',
      author_email='g.seyerl@geoase.eu',
      packages=setuptools.find_packages(),
      keywords=['climate', ],
      # tests_require=['pytest'],
     )
