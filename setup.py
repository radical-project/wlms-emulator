#!/usr/bin/env python

__author__ = "Vivek Balasubramanian"
__email__ = "vivek.balasubramanian@rutgers.edu"
__copyright__ = "Copyright 2018, The RADICAL Project at Rutgers"
__license__ = "MIT"


""" Setup script. Used by easy_install and pip. """

import sys
name = 'radical.calculator'

try:
    from setuptools import setup, Command, find_packages
except ImportError as e:
    print("%s needs setuptools to install" % name)
    sys.exit(1)

# -----------------------------------------------------------------------------
#
if sys.hexversion < 0x02060000 or sys.hexversion >= 0x03000000:
    raise RuntimeError(
        "SETUP ERROR: radical.entk requires Python 2.6 or higher")

# short_version = 0.6

setup_args = {
    'name': 'radical.calculator',
    'version': 0.1,
    'description': "Execution profile calculator",
    # 'long_description' : (read('README.md') + '\n\n' + read('CHANGES.md')),
    'author': 'RADICAL Group at Rutgers University',
    'author_email': 'vivek.balasubramanian@rutgers.edu',
    'maintainer': "Vivek Balasubramanian",
    'maintainer_email': 'vivek.balasubramanian@rutgers.edu',
    'url': 'https://github.com/vivek-bala/calculator',
    'license': 'MIT',
    'keywords': "workload resource execution",
    'classifiers':  [
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Environment :: Console',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.5',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Topic :: Utilities',
        'Topic :: System :: Distributed Computing',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX',
        'Operating System :: Unix'
    ],

    'scripts': ['bin/start-wlms',
                'bin/start-executor'],
    # 'namespace_packages': ['radical', 'radical'],
    'packages': find_packages('src'),
    'package_dir': {'': 'src'},
    'package_data':  {'': ['*.sh', '*.json', 'VERSION', 'SDIST']},
    'install_requires':  ['radical.utils', 'numpy', 'pytest', 'pyyaml',
                          'hypothesis', 'pylint', 'flake8', 'pika',
                          'pandas','matplotlib'],

    'zip_safe': False
}

setup(**setup_args)


'''
To publish to pypi:
python setup.py sdist
twine upload --skip-existing dist/<tarball name>
'''
