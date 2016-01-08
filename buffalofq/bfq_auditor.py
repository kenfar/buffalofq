#!/usr/bin/env python

from __future__ import division
import os
import sys
from os.path import join as pjoin
from os.path import isfile, isdir, exists, dirname, basename

import yaml
import time
import logging
from pprint import pprint as pp

logger = None


class FeedAuditor(object):
    """ Writes status for every step for every feed,
        reads data back.
	Used to determine when to next poll as well as to help
	in recoveries.
    """
    def __init__(self, feed_name, data_dir, config_name):

        setup_logging('__main__')
        logger.debug('FeedAuditor starting now')

        self.feed_name       = feed_name
        self.data_dir        = data_dir
        self.audit_fqfn      = pjoin(data_dir, '%s_audit.yml' % config_name)
        self.start_time      = time.time()
        logger.info('audit_fqfn: %s', self.audit_fqfn)
        if not isdir(dirname(self.audit_fqfn)):
            logger.critical('Invalid audit_fqfn - dir does not exist: %s' % self.audit_fqfn)
            sys.exit(1)

        self.all_feed_status = self.read()
        self.status          = self.all_feed_status[self.feed_name]   # just a shortcut
        if (self.status['step'] in [0, 6]
            and self.status['status'] == 'stop'
            and self.status['result'] == 'pass'):
            self.mode            = 'normal'
        else:
            self.mode            = 'recovery'

    def read(self):
	"""Read entire feed_status yaml file into class dictionary.
           If the file doesn't exist, set up a minimal default.
	"""
        try:
            with open(self.audit_fqfn, 'r') as f:
                return yaml.safe_load(f)
        except IOError, e:
            if e.errno == 2:
                feed_status_rec = {}
                feed_status_rec[self.feed_name] = {'step'       : 0,
                                                   'status'     : 'stop',
                                                   'time'       : time.time(),
                                                   'result'     : 'pass',
                                                   'fn'         : '',
                                                   'empty_audit': True}
                return feed_status_rec
            else:
                logger.critical('feed audit file could not be loaded')
                logger.critical(e)
                sys.exit(1)

    def write(self, step, status, result='tbd', fn=None):
        assert 10 > step >= 0
        assert status in ['start','stop']
        assert result in ['pass', 'fail', 'tbd', True, False]
        if result == True:
            result = 'pass'
        elif result == False:
            result = 'fail'
        if status == 'start':
            result = 'tbd'

        self.status['step']        = step
        self.status['status']      = status
        self.status['time']        = time.time()
        self.status['result']      = result
        if fn is not None:
            self.status['fn']      = fn
        self.status['empty_audit'] = False

        #print self.status
        with open(self.audit_fqfn, 'w') as f:
            f.write(yaml.dump(self.all_feed_status))



def setup_logging(log_name):
    global logger
    logger = logging.getLogger(log_name + '.auditor')



