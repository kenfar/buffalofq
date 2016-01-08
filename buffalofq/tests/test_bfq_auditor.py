#!/usr/bin/env python

import sys
import os
import tempfile
import imp

sys.path.insert(1, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import buffalofq.bfq_auditor as mod


class TestFeedAuditor(object):

    def setup_method(self, method):
        self.audit_dir     = tempfile.mkdtemp(prefix='buffalofq_ad_')
        self.FeedAuditor   = mod.FeedAuditor(feed_name='test',
                                             data_dir=self.audit_dir,
                                             config_name='buffalofq.yml')

    def test_good_to_go_startup(self):
        assert self.FeedAuditor.status['step']   == 0
        assert self.FeedAuditor.status['result'] == 'pass'
        #assert self.FeedAuditor.good_to_run(1) is True
        #assert self.FeedAuditor.good_to_run(2) is False
        #assert self.FeedAuditor.good_to_run(3) is False
        #assert self.FeedAuditor.good_to_run(4) is False

    def test_good_to_go_step2(self):
        self.FeedAuditor.status['step']   = 1
        self.FeedAuditor.status['result'] = 'pass'
        #assert self.FeedAuditor.good_to_run(1) is False
        #assert self.FeedAuditor.good_to_run(2) is True
        #assert self.FeedAuditor.good_to_run(3) is False
        #assert self.FeedAuditor.good_to_run(4) is False
        #assert self.FeedAuditor.good_to_run(5) is False

    def test_good_to_go_step4(self):
        self.FeedAuditor.status['step']   = 3
        self.FeedAuditor.status['result'] = 'pass'
        #assert self.FeedAuditor.good_to_run(1) is False
        #assert self.FeedAuditor.good_to_run(2) is False
        #assert self.FeedAuditor.good_to_run(3) is False
        #assert self.FeedAuditor.good_to_run(4) is True
        #assert self.FeedAuditor.good_to_run(5) is False







