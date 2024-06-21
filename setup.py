#!/usr/bin/env python

from distutils.core import setup

setup(name='Alpha Compiler',
      version='0.2',
      description='Python tools for quantitative finance',
      author='Peter Harrington & Max Wimmer',
      author_email='',
      url='',
      packages=['alphacompiler',
          'alphacompiler.util', 
          'alphacompiler.data', 
          'alphacompiler.data.loaders'], requires=['pandas', 'zipline', 'numpy']
      )
