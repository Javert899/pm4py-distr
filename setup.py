from os.path import dirname, join

from setuptools import setup

import pm4pydistr


def read_file(filename):
    with open(join(dirname(__file__), filename)) as f:
        return f.read()


setup(
    name=pm4pydistr.__name__,
    version=pm4pydistr.__version__,
    description=pm4pydistr.__doc__.strip(),
    long_description=read_file('README.md'),
    author=pm4pydistr.__author__,
    author_email=pm4pydistr.__author_email__,
    py_modules=[pm4pydistr.__name__],
    include_package_data=True,
    packages=['pm4pydistr', 'pm4pydistr.util', 'pm4pydistr.slave', 'pm4pydistr.master', 'pm4pydistr.master.rqsts',
              'pm4pydistr.log_handlers', 'pm4pydistr.log_handlers.parquet_filtering',
              'pm4pydistr.log_handlers.parquet_filtering.versions', 'pm4pydistr.local_wrapper',
              'pm4pydistr.local_wrapper.versions', 'pm4pydistr.remote_wrapper', 'pm4pydistr.remote_wrapper.versions'],
    url='http://www.pm4py.org',
    license='GPL 3.0',
    install_requires=[
        'pm4py',
        'requests',
        'Flask',
        'flask-cors',
        'psutil'
    ],
    project_urls={
        'Documentation': 'http://pm4py.pads.rwth-aachen.de/documentation/',
        'Source': 'https://github.com/pm4py/pm4py-distr',
        'Tracker': 'https://github.com/pm4py/pm4py-distr/issues',
    }
)
