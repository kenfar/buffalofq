#!/usr/bin/env python2

import os, uuid
from pip.req import parse_requirements
from setuptools import setup, find_packages

def read(*paths):
    with open(os.path.join(*paths), 'r') as f:
        return f.read()

def get_version():
    v1_rec = read("buffalofq/_version.py")
    (v1_label, v1_version) = v1_rec.split('=')
    v2_version = v1_version.strip()[1:-1]
    assert v1_label.strip() == '__version__'
    assert v2_version.count('.') == 2
    return v2_version

VERSION      = get_version()
DESCRIPTION  = "File movement utility"
REQUIREMENTS = [str(ir.req) for ir in parse_requirements('requirements.txt', session=uuid.uuid1())]

setup(name              = 'buffalofq',
      version           = VERSION,
      description       = DESCRIPTION,
      long_description  = (read('README.rst')
                           + '\n\n'
                           + read('CHANGELOG.rst')),
      keywords          = "data management file utility",
      author            = "Ken Farmer",
      author_email      = "kenfar@gmail.com",
      url               = 'http://github.com/kenfar/buffalofq',
      license           = 'BSD',
      classifiers       = ['Programming Language :: Python :: 2.7',
                           'Programming Language :: Python :: Implementation :: CPython',
                           'Programming Language :: Python :: Implementation :: PyPy'],
      install_requires  = REQUIREMENTS,
      packages          = find_packages(),
      scripts           = [ 'scripts/buffalofq']
      )
