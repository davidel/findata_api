#!/usr/bin/env python3

from setuptools import setup, find_packages


setup(name='findata_api',
      version='0.1.1',
      description='Common API to access financial data',
      author='Davide Libenzi',
      packages=find_packages(),
      package_data={
          'findata_api': [
              # Paths from findata_api/ subfolder ...
          ],
      },
      include_package_data=True,
      install_requires=[
          'numpy',
          'pandas',
          'orjson',
          'python_misc_utils',
          'pandas_market_calendars',
          'websocket-client',
      ],
      )

