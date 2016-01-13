#!/usr/bin/env python

import os
import imp
import glob
import tempfile
import shutil
from os.path import dirname, basename, exists, isfile, isdir, join as pjoin


def load_script(script):
    """ Imports script from parent directory.  Has following features:
        - can handle scripts with or without .py suffix
        - should support being run from different directories
    """
    test_dir = os.path.dirname(os.path.realpath(__file__))
    script_dir = os.path.dirname(test_dir)
    py_source_open_mode = "U"
    py_source_description = (".py", py_source_open_mode, imp.PY_SOURCE)
    script_filepath = pjoin(script_dir, script)
    with open(script_filepath, py_source_open_mode) as script_file:
        mod = imp.load_module(script, script_file, script_filepath, py_source_description)
    return mod


def remove_dir(dir):
    if os.path.exists(dir):
        shutil.rmtree(dir)


def remove_all_buffalofq_temp_dirs():
    """ The objective of this code is to eliminate directories left over from prior
        test runs - that crashed before the temp dirs could be removed.
    """
    ###[os.remove(f) for f in glob.glob(pjoin(self.feed_audit_dir, '*'))]
    for file in glob.glob(pjoin(tempfile.gettempdir(), 'bfq_*')):
        shutil.rmtree(file)



