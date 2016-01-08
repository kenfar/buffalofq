#!/usr/bin/env python

import sys
import os
import tempfile
import shutil
import imp
import glob
import logging
from pprint import pprint as pp

import test_tools

mod = test_tools.load_script('buffalofq')

verbose = False


def setup_logging():
    logger   = logging.getLogger('bfq')

    #---- create format ----------
    log_format  = '%(asctime)s : %(name)-12s : %(levelname)-8s : %(message)s'
    date_format = '%Y-%m-%d %H.%M.%S'
    formatter   = logging.Formatter(log_format, date_format)

    #---- add handlers ----------
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    logger.setLevel(logging.INFO)
    mod.logger = logger





