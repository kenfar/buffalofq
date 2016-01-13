#!/usr/bin/env python

import sys, os
import getpass
import tempfile
import shutil
import imp
import glob
import logging
from pprint import pprint as pp
from os.path import dirname, basename, exists, isdir, isfile, join as pjoin

sys.path.insert(1, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import bfq_test_tools  as test_tools
import buffalofq.bfq_buffguts as mod

verbose = False
SOURCE_USER = getpass.getuser()
DEST_USER   = getpass.getuser()


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



class TestLocalToRemoteCopy(object):

    def setup_method(self, method):
        test_tools.remove_all_buffalofq_temp_dirs()
        self.source_data_dir = tempfile.mkdtemp(prefix='bfq_sd_')
        self.source_arc_dir  = tempfile.mkdtemp(prefix='bfq_sa_')
        self.dest_data_dir   = tempfile.mkdtemp(prefix='bfq_dd_')
        self.dest_link_dir   = tempfile.mkdtemp(prefix='bfq_dl_')
        self.feed_audit_dir  = tempfile.mkdtemp(prefix='bfq_fa_')
        self.config_dir      = tempfile.mkdtemp(prefix='bfq_cd_')
        _make_file(self.source_data_dir,  'good')
        _make_file(self.source_data_dir,  'good')
        _make_file(self.source_data_dir,  'good')
        _make_file(self.source_data_dir,  'bad')
        _make_file(self.source_data_dir,  'bad')
        _make_file(self.source_arc_dir,   'ignore')
        _make_file(self.dest_data_dir,    'ignore')
        _make_file(self.dest_link_dir,    'ignore')

        self.dir_files = {}
        setup_logging()


    def teardown_method(self, method):
        test_tools.remove_all_buffalofq_temp_dirs()


    def test_file_sorting_by_None(self):
        feed = _make_default_feed(self.source_data_dir, self.dest_data_dir)
        feed['sort_key'] = None
        OneFeed = mod.HandleOneFeed(feed, self.feed_audit_dir, limit_total=0,
                                    config_name=None, key_filename='id_buffalofq_rsa')
        files = ['f', 'e', 'd', 'c', 'b', 'a']
        assert OneFeed._sort_files(files) == ['f', 'e', 'd', 'c', 'b', 'a']

        files = []
        assert OneFeed._sort_files(files) == []

        OneFeed.close()


    def test_file_sorting_by_name(self):
        feed = _make_default_feed(self.source_data_dir, self.dest_data_dir)
        feed['sort_key'] = 'name'
        OneFeed = mod.HandleOneFeed(feed, self.feed_audit_dir, limit_total=0,
                                    config_name=None, key_filename='id_buffalofq_rsa')
        files = ['f', 'e', 'd', 'c', 'b', 'a']
        assert OneFeed._sort_files(files) == ['a', 'b', 'c', 'd', 'e', 'f']

        files = []
        assert OneFeed._sort_files(files) == []

        OneFeed.close()



    def test_file_sorting_by_key(self):
        feed = _make_default_feed(self.source_data_dir, self.dest_data_dir)
        OneFeed = mod.HandleOneFeed(feed, self.feed_audit_dir, limit_total=0,
                                    config_name=None, key_filename='id_buffalofq_rsa')
        OneFeed.feed['sort_key'] = 'field:date'
        files = ['foo_date-2015.csv', 'bar_date-2016.csv', 'mook_date-2014.csv']
        assert OneFeed._sort_files(files) == ['mook_date-2014.csv', 'foo_date-2015.csv', 'bar_date-2016.csv']

        files = []
        assert OneFeed._sort_files(files) == []

        OneFeed.close()



    def test_copy_many_files(self):
        """ Tests copying many files from source to dest
            AND leaving source files alone afterwards
            AND ignoring other files in all directories.
        """
        print
        print('======================== Test: happypath ==========================')
        feed = _make_default_feed(self.source_data_dir, self.dest_data_dir)

        OneFeed = mod.HandleOneFeed(feed, self.feed_audit_dir, limit_total=0,
                                    config_name=None, key_filename='id_buffalofq_rsa')
        OneFeed.run(force=True)
        OneFeed.close()

        assert len(glob.glob(pjoin(self.source_data_dir,'good*'))) > 0
        assert len(glob.glob(pjoin(self.source_data_dir,'bad*')))  > 0

        assert len(glob.glob(pjoin(self.source_arc_dir,'good*')))   == 0
        assert len(glob.glob(pjoin(self.source_arc_dir,'bad*')))    == 0
        assert len(glob.glob(pjoin(self.source_arc_dir,'ignore*'))) > 0

        assert len(glob.glob(pjoin(self.dest_data_dir,'good*')))    > 0
        assert len(glob.glob(pjoin(self.dest_data_dir,'bad*')))     == 0


    def test_source_post_action_delete(self):
        """ Tests copying many files from source to dest
            AND deleting source files
            AND ignoring other files in all directories.
        """
        feed = _make_default_feed(self.source_data_dir, self.dest_data_dir)
        feed['source_post_dir']    = ''
        feed['source_post_action'] = 'delete'

        OneFeed = mod.HandleOneFeed(feed, self.feed_audit_dir, limit_total=0,
                                    config_name=None, key_filename='id_buffalofq_rsa')
        OneFeed.run(force=True)
        OneFeed.close()

        # first make this feature doesn't break any other logic:
        assert len(glob.glob(pjoin(self.dest_data_dir,'good*')))    > 0
        assert len(glob.glob(pjoin(self.dest_data_dir,'bad*')))     == 0
        assert len(glob.glob(pjoin(self.dest_data_dir,'ignore*')))  > 0

        # next - lets see if it worked right:
        assert len(glob.glob(pjoin(self.source_data_dir,'good*'))) == 0
        assert len(glob.glob(pjoin(self.source_data_dir,'bad*')))  > 0
        assert len(glob.glob(pjoin(self.source_arc_dir,'good*')))   == 0
        assert len(glob.glob(pjoin(self.source_arc_dir,'bad*')))    == 0
        assert len(glob.glob(pjoin(self.source_arc_dir,'ignore*'))) > 0



    def test_source_post_action_move(self):
        """ Tests copying many files from source to dest
            AND archiving source files
            AND ignoring other files in all directories.
        """
        feed = _make_default_feed(self.source_data_dir, self.dest_data_dir)
        feed['source_post_dir']    = self.source_arc_dir
        feed['source_post_action'] = 'move'

        OneFeed = mod.HandleOneFeed(feed, self.feed_audit_dir, limit_total=0,
                                    config_name=None, key_filename='id_buffalofq_rsa')
        OneFeed.run(force=True)
        OneFeed.close()

        # first make this feature doesn't break any other logic:
        assert len(glob.glob(pjoin(self.dest_data_dir,'good*')))    > 0
        assert len(glob.glob(pjoin(self.dest_data_dir,'bad*')))     == 0
        assert len(glob.glob(pjoin(self.dest_data_dir,'ignore*')))  > 0

        # next - lets see if it moved the source:
        assert len(glob.glob(pjoin(self.source_data_dir,'good*'))) == 0
        assert len(glob.glob(pjoin(self.source_data_dir,'bad*')))  > 0
        assert len(glob.glob(pjoin(self.source_arc_dir,'good*')))   > 0
        assert len(glob.glob(pjoin(self.source_arc_dir,'bad*')))    == 0
        assert len(glob.glob(pjoin(self.source_arc_dir,'ignore*'))) > 0



    def test_dest_post_action_symlink(self):
        """ Tests copying many files from source to dest
            AND ignoring other files in all directories.
            AND symlinking dest files to post_dest dir
        """
        feed = _make_default_feed(self.source_data_dir, self.dest_data_dir)
        feed['dest_post_action'] = 'symlink'
        feed['dest_post_dir'] = self.dest_link_dir
        feed['dest_post_fn']  = 'good_link'

        OneFeed = mod.HandleOneFeed(feed, self.feed_audit_dir, limit_total=0,
                                    config_name=None, key_filename='id_buffalofq_rsa')
        OneFeed.run(force=True)
        OneFeed.close()

        # first make sure symlink doesn't break any other logic:
        assert len(glob.glob(pjoin(self.source_data_dir,'good*'))) > 0
        assert len(glob.glob(pjoin(self.source_data_dir,'bad*')))  > 0
        assert len(glob.glob(pjoin(self.source_arc_dir,'good*')))   == 0
        assert len(glob.glob(pjoin(self.source_arc_dir,'bad*')))    == 0
        assert len(glob.glob(pjoin(self.source_arc_dir,'ignore*'))) > 0
        assert len(glob.glob(pjoin(self.dest_data_dir,'good*')))    > 0
        assert len(glob.glob(pjoin(self.dest_data_dir,'bad*')))     == 0
        assert len(glob.glob(pjoin(self.dest_data_dir,'ignore*')))  > 0

        # next is the primary check for this test
        print(os.listdir(self.dest_link_dir))
        assert len(glob.glob(pjoin(self.dest_link_dir,'good_link*')))  == 1
        # finally - consider cat'ing results and comparing to original to confirm
        # that it's a working symlink!



class TestTaskRecovery(object):

    def setup_method(self, method):
        setup_logging()
        test_tools.remove_all_buffalofq_temp_dirs()
        self.feed_audit_dir  = tempfile.mkdtemp(prefix='bfq_feed_audit_')

        self.dirs = {}
        self.dirs['source_data'] = {}
        self.dirs['source_data']['name'] = tempfile.mkdtemp(prefix='bfq_source_data_')
        self.dirs['source_data']['good'] = 3
        self.dirs['source_data']['bad']  = 2
        self.dirs['source_arc']            = {}
        self.dirs['source_arc']['name']    = tempfile.mkdtemp(prefix='bfq_source_arc_')
        self.dirs['source_arc']['ignore']  = 1
        self.dirs['source_arc']['good']    = 0
        self.dirs['dest_data']            = {}
        self.dirs['dest_data']['name']    = tempfile.mkdtemp(prefix='bfq_dest_data_')
        self.dirs['dest_data']['ignore']  = 1
        self.dirs['dest_data']['good']    = 0
        self.dirs['dest_link']            = {}
        self.dirs['dest_link']['name']    = tempfile.mkdtemp(prefix='bfq_dest_link_')
        self.dirs['dest_link']['ignore']  = 1

        for dir_key in self.dirs.keys():
            for file_type in self.dirs[dir_key].keys():
                if file_type != 'name':
                    for i in range(self.dirs[dir_key][file_type]):
                        #print pjoin(self.dirs[dir]['name'], file_type)
                        _make_file(self.dirs[dir_key]['name'], file_type)

        self.step = {}
        self.step[1] = {}
        self.step[1]['a'] = {'fail_mode':'normal',   'fail_status':'stop'}
        self.step[1]['b'] = {'fail_mode':'normal',   'fail_status':'stop'}
        self.step[1]['c'] = {'fail_mode':'recovery', 'fail_status':'start'}
        self.step[1]['d'] = {'fail_mode':'recovery', 'fail_status':'start'}
        self.step[1]['e'] = {'fail_mode':'recovery', 'fail_status':'stop'}
        self.step[2] = {}
        self.step[2]['a'] = {'fail_mode':'recovery', 'fail_status':'stop'}
        self.step[2]['b'] = {'fail_mode':'recovery', 'fail_status':'stop'}
        self.step[2]['c'] = {'fail_mode':'recovery', 'fail_status':'start'}
        self.step[2]['d'] = {'fail_mode':'recovery', 'fail_status':'start'}
        self.step[2]['e'] = {'fail_mode':'recovery', 'fail_status':'stop'}
        self.step[3] = {}
        self.step[3]['a'] = {'fail_mode':'recovery', 'fail_status':'stop'}
        self.step[3]['b'] = {'fail_mode':'recovery', 'fail_status':'stop'}
        self.step[3]['c'] = {'fail_mode':'recovery', 'fail_status':'start'}
        self.step[3]['d'] = {'fail_mode':'recovery', 'fail_status':'start'}
        self.step[3]['e'] = {'fail_mode':'recovery', 'fail_status':'stop'}
        self.step[4] = {}
        self.step[4]['a'] = {'fail_mode':'recovery', 'fail_status':'stop'}
        self.step[4]['b'] = {'fail_mode':'recovery', 'fail_status':'stop'}
        self.step[4]['c'] = {'fail_mode':'recovery', 'fail_status':'start'}
        self.step[4]['d'] = {'fail_mode':'recovery', 'fail_status':'start'}
        self.step[4]['e'] = {'fail_mode':'recovery', 'fail_status':'stop'}
        self.step[5] = {}
        self.step[5]['a'] = {'fail_mode':'recovery', 'fail_status':'stop'}
        self.step[5]['b'] = {'fail_mode':'recovery', 'fail_status':'stop'}
        self.step[5]['c'] = {'fail_mode':'recovery', 'fail_status':'start'}
        self.step[5]['d'] = {'fail_mode':'recovery', 'fail_status':'start'}
        self.step[5]['e'] = {'fail_mode':'recovery', 'fail_status':'stop'}
        self.step[6] = {}
        self.step[6]['a'] = {'fail_mode':'recovery', 'fail_status':'stop'}
        self.step[6]['b'] = {'fail_mode':'recovery', 'fail_status':'stop'}
        self.step[6]['c'] = {'fail_mode':'recovery', 'fail_status':'start'}
        self.step[6]['d'] = {'fail_mode':'recovery', 'fail_status':'start'}
        self.step[6]['e'] = {'fail_mode':'normal'  , 'fail_status':'stop'}


    def teardown_method(self, method):
        test_tools.remove_all_buffalofq_temp_dirs()

    def _assert_dir_changed_by_n(self, dir, file_type, n=0):
        assert self._get_file_count(dir, '%s*' % file_type) == self.dirs[dir][file_type] + n

    def _get_file_count(self, dir_name, file_name):
        real_file_list = [x for x in glob.glob(pjoin(self.dirs[dir_name]['name'], file_name))
                          if os.path.isfile(x)]

        return len(real_file_list)

    def run_1_of_2(self, failstep=None, failsubstep=None, failcatch=False):
        """ Runs HandleOneFeed through a single file, causing it to fail at the point specified
            by its arguments.
        """
        assert self._get_file_count('source_data', 'good*') == 3
        assert self._get_file_count('source_arc', 'good*') == 0

        print
        print('======================== Step: %s%s ==========================' % (failstep, failsubstep))
        self.print_files()

        self.feed = _make_default_feed(self.dirs['source_data']['name'],
                                       self.dirs['dest_data']['name'])
        self.feed['source_post_dir']    = self.dirs['source_arc']['name']
        self.feed['source_post_action'] = 'move'

        mod.FAIL_STEP    = failstep
        mod.FAIL_SUBSTEP = failsubstep
        mod.FAIL_CATCH   = failcatch
        OneFeed = mod.HandleOneFeed(self.feed, self.feed_audit_dir, limit_total=0,
                                    config_name=None, key_filename='id_buffalofq_rsa')
        try:
            OneFeed.run(force=True)
        except SystemExit:
            print '~~~~~~ test_recovery - systemexit ~~~~~~~~'
        self.broken_file = OneFeed.auditor.status['fn']

        # failstep not provided if function is to be run without crashing
        if failstep:
            if failstep == 6 and failsubstep in ['d','e']:
                self._assert_dir_changed_by_n('source_data', 'good', n=-1)
                self._assert_dir_changed_by_n('source_arc', 'good', n=1)
                assert self._get_file_count('source_data', self.broken_file) == 0
            elif failstep == 1 and failsubstep in ['a','b']:
                self._assert_dir_changed_by_n('source_data', 'good', n=0)
                self._assert_dir_changed_by_n('source_arc', 'good', n=0)
                assert self._get_file_count('source_data', self.broken_file) == 0
            else:
                self._assert_dir_changed_by_n('source_data', 'good', n=0)
                self._assert_dir_changed_by_n('source_arc', 'good', n=0)
                assert self._get_file_count('source_data', self.broken_file) == 1
            if failstep < 4:
                destlist = glob.glob(pjoin(self.dirs['dest_data']['name'],'good*'))
                for file_name in destlist:
                    assert file_name.endswith('.temp')

            # ensure feed status is right
            if failsubstep in ['a','b']:
                assert OneFeed.auditor.status['step']   == failstep - 1
                assert OneFeed.auditor.status['status'] == 'stop'
                assert OneFeed.auditor.status['result'] == 'pass'
                #assert self.step[failstep][failsubstep]['fail_status'] \
                #   == OneFeed.auditor.status['status']
            elif failsubstep == 'e':
                assert OneFeed.auditor.status['step']   == failstep
                assert OneFeed.auditor.status['status'] == 'stop'
                assert OneFeed.auditor.status['result'] == 'pass'
                #assert self.step[failstep][failsubstep]['fail_status'] \
                #   == OneFeed.auditor.status['status']
            elif failsubstep == 'd' and failcatch:
                assert OneFeed.auditor.status['step']   == failstep
                #assert OneFeed.auditor.status['status'] == 'stop'
                assert OneFeed.auditor.status['result'] == 'fail'
            else:
                assert OneFeed.auditor.status['step']   == failstep
                assert OneFeed.auditor.status['status'] == 'start'
                assert OneFeed.auditor.status['result'] == 'tbd'
                #assert self.step[failstep][failsubstep]['fail_status'] \
                #   == OneFeed.auditor.status['status']

        OneFeed.close()


    def run_2_of_2(self, failstep=None, failsubstep=None, failcatch=False):
        """ Runs HandleOneFeed for a single feed that was typically previously in a recovery mode.
        """
        mod.FAIL_STEP    = -1
        mod.FAIL_SUBSTEP = -1
        mod.FAIL_CATCH   = failcatch
        OneFeed = mod.HandleOneFeed(self.feed, self.feed_audit_dir, limit_total=0,
                                    config_name=None, key_filename='id_buffalofq_rsa')
        self.feed['source_post_dir']    = self.dirs['source_arc']['name']
        self.feed['source_post_action'] = 'move'
        OneFeed.run(force=True)

        if not failstep:
            recovery_ran = False
        elif self.step[failstep][failsubstep]['fail_mode'] == 'normal':
            recovery_ran = False
        else:
            recovery_ran = True

        self.print_files()

        if recovery_ran:
            # recovery second job will only process the 1 broken file!
            assert len(glob.glob(pjoin(self.dirs['source_data']['name'],'good*'))) == 2
            assert len(glob.glob(pjoin(self.dirs['source_data']['name'], self.broken_file))) == 0
            assert len(glob.glob(pjoin(self.dirs['source_arc']['name'],'good*')))  == 1
            assert len(glob.glob(pjoin(self.dirs['dest_data']['name'],'good*')))   == 1
        else:  # non-recovery second job will have processed all files
            assert len(glob.glob(pjoin(self.dirs['source_data']['name'],'good*'))) == 0
            if self.broken_file: # could be '' on a non-recovery
                assert len(glob.glob(pjoin(self.dirs['source_data']['name'], self.broken_file))) == 0
            assert len(glob.glob(pjoin(self.dirs['source_arc']['name'],'good*')))  == 3
            assert len(glob.glob(pjoin(self.dirs['dest_data']['name'],'good*')))   == 3

        # ensure feed status is right
        if failstep:
            assert OneFeed.auditor.status['step']   == 6
        else:
            assert OneFeed.auditor.status['step']   == 0
        assert OneFeed.auditor.status['status'] == 'stop'
        assert OneFeed.auditor.status['result'] == 'pass'

        OneFeed.close()



    def print_files(self):
        if verbose:
            print 'source_data_dir:'
            pp(glob.glob(pjoin(self.dirs['source_data']['name'],'*')), indent=4)
            print 'source_arc_dir:'
            pp(glob.glob(pjoin(self.dirs['source_arc']['name'],'*')), indent=4)
            print 'dest_data_dir:'
            pp(glob.glob(pjoin(self.dirs['dest_data']['name'],'*')), indent=4)
            print

    def test_recovery_all_steps_no_failure(self):
        self.run_1_of_2() # everything worked great!
        self.run_2_of_2() # everything worked great!

    def test_recovery_step1a(self):
        self.run_1_of_2(failstep=1, failsubstep='a')
        self.run_2_of_2(failstep=1, failsubstep='a')

    def test_recovery_step1b(self):
        self.run_1_of_2(failstep=1, failsubstep='b')
        self.run_2_of_2(failstep=1, failsubstep='b')

    def test_recovery_step1c(self):
        self.run_1_of_2(failstep=1, failsubstep='c')
        self.run_2_of_2(failstep=1, failsubstep='c')

    def test_recovery_step1d(self):
        self.run_1_of_2(failstep=1, failsubstep='d')
        self.run_2_of_2(failstep=1, failsubstep='d')

    def test_recovery_step1d2(self):
        self.run_1_of_2(failstep=1, failsubstep='d', failcatch=True)
        self.run_2_of_2(failstep=1, failsubstep='d', failcatch=True)

    def test_recovery_step1e(self):
        self.run_1_of_2(failstep=1, failsubstep='e')
        self.run_2_of_2(failstep=1, failsubstep='e')


    def test_recovery_step2a(self):
        self.run_1_of_2(failstep=2, failsubstep='a')
        self.run_2_of_2(failstep=2, failsubstep='a')

    def test_recovery_step2b(self):
        self.run_1_of_2(failstep=2, failsubstep='b')
        self.run_2_of_2(failstep=2, failsubstep='b')

    def test_recovery_step2c(self):
        self.run_1_of_2(failstep=2, failsubstep='c')
        self.run_2_of_2(failstep=2, failsubstep='c')

    def test_recovery_step2d(self):
        self.run_1_of_2(failstep=2, failsubstep='d')
        self.run_2_of_2(failstep=2, failsubstep='d')

    def test_recovery_step2d2(self):
        self.run_1_of_2(failstep=2, failsubstep='d', failcatch=True)
        self.run_2_of_2(failstep=2, failsubstep='d', failcatch=True)

    def test_recovery_step2e(self):
        self.run_1_of_2(failstep=2, failsubstep='e')
        self.run_2_of_2(failstep=2, failsubstep='e')


    def test_recovery_step3a(self):
        self.run_1_of_2(failstep=3, failsubstep='a')
        self.run_2_of_2(failstep=3, failsubstep='a')

    def test_recovery_step3b(self):
        self.run_1_of_2(failstep=3, failsubstep='b')
        self.run_2_of_2(failstep=3, failsubstep='b')

    def test_recovery_step3c(self):
        self.run_1_of_2(failstep=3, failsubstep='c')
        self.run_2_of_2(failstep=3, failsubstep='c')

    def test_recovery_step3d(self):
        self.run_1_of_2(failstep=3, failsubstep='d')
        self.run_2_of_2(failstep=3, failsubstep='d')

    def test_recovery_step3d2(self):
        self.run_1_of_2(failstep=3, failsubstep='d', failcatch=True)
        self.run_2_of_2(failstep=3, failsubstep='d', failcatch=True)

    def test_recovery_step3e(self):
        self.run_1_of_2(failstep=3, failsubstep='e')
        self.run_2_of_2(failstep=3, failsubstep='e')


    def test_recovery_step4a(self):
        self.run_1_of_2(failstep=4, failsubstep='a')
        self.run_2_of_2(failstep=4, failsubstep='a')

    def test_recovery_step4b(self):
        self.run_1_of_2(failstep=4, failsubstep='b')
        self.run_2_of_2(failstep=4, failsubstep='b')

    def test_recovery_step4c(self):
        self.run_1_of_2(failstep=4, failsubstep='c')
        self.run_2_of_2(failstep=4, failsubstep='c')

    def test_recovery_step4d(self):
        self.run_1_of_2(failstep=4, failsubstep='d')
        self.run_2_of_2(failstep=4, failsubstep='d')

    def test_recovery_step4d2(self):
        self.run_1_of_2(failstep=4, failsubstep='d', failcatch=True)
        self.run_2_of_2(failstep=4, failsubstep='d', failcatch=True)

    def test_recovery_step4e(self):
        self.run_1_of_2(failstep=4, failsubstep='e')
        self.run_2_of_2(failstep=4, failsubstep='e')


    def test_recovery_step5a(self):
        self.run_1_of_2(failstep=5, failsubstep='a')
        self.run_2_of_2(failstep=5, failsubstep='a')

    def test_recovery_step5b(self):
        self.run_1_of_2(failstep=5, failsubstep='b')
        self.run_2_of_2(failstep=5, failsubstep='b')

    def test_recovery_step5c(self):
        self.run_1_of_2(failstep=5, failsubstep='c')
        self.run_2_of_2(failstep=5, failsubstep='c')

    def test_recovery_step5d(self):
        self.run_1_of_2(failstep=5, failsubstep='d')
        self.run_2_of_2(failstep=5, failsubstep='d')

    def test_recovery_step5d2(self):
        self.run_1_of_2(failstep=5, failsubstep='d', failcatch=True)
        self.run_2_of_2(failstep=5, failsubstep='d', failcatch=True)

    def test_recovery_step5e(self):
        self.run_1_of_2(failstep=5, failsubstep='e')
        self.run_2_of_2(failstep=5, failsubstep='e')


    def test_recovery_step6a(self):
        self.run_1_of_2(failstep=6, failsubstep='a')
        self.run_2_of_2(failstep=6, failsubstep='a')

    def test_recovery_step6b(self):
        self.run_1_of_2(failstep=6, failsubstep='b')
        self.run_2_of_2(failstep=6, failsubstep='b')

    def test_recovery_step6c(self):
        self.run_1_of_2(failstep=6, failsubstep='c')
        self.run_2_of_2(failstep=6, failsubstep='c')

    def test_recovery_step6d1(self):
        self.run_1_of_2(failstep=6, failsubstep='d')
        self.run_2_of_2(failstep=6, failsubstep='d')

    def test_recovery_step6d2(self):
        self.run_1_of_2(failstep=6, failsubstep='d', failcatch=True)
        self.run_2_of_2(failstep=6, failsubstep='d', failcatch=True)

    def test_recovery_step6e(self):
        self.run_1_of_2(failstep=6, failsubstep='e')
        self.run_2_of_2(failstep=6, failsubstep='e')






def _make_default_feed(source_data_dir, dest_data_dir):
    feed = {}
    feed['name']            = 'source_2_dest'
    feed['status']          = 'enabled'
    feed['polling_seconds'] = '10'
    feed['sort_key']        = 'name'
    feed['source_host']     = 'localhost'
    feed['source_user']     = SOURCE_USER
    feed['source_dir']      = source_data_dir
    feed['source_fn']       = 'good*'
    feed['dest_host']       = 'localhost'
    feed['dest_user']       = DEST_USER
    feed['dest_dir']        = dest_data_dir
    feed['dest_fn']         = ''
    feed['port']            = 22
    feed['source_post_action'] = ''
    feed['source_post_dir']    = ''
    return feed

def _make_file(dir, prefix):
    adjusted_prefix = '%s_' % prefix
    (fd, fqfn) = tempfile.mkstemp(dir=dir, prefix=adjusted_prefix, suffix='.dat')
    fp = os.fdopen(fd,"w")
    fp.write('1234567890\n')
    fp.write('234567890\n')
    fp.write('34567890\n')
    fp.write('4567890\n')
    fp.write('567890\n')
    fp.close()
    return fqfn


