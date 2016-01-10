#!/usr/bin/env python
""" Tests buffalofq from the commandline rather than reaching in and testing its
    classes and functions directly.

    Tests include:
      - TestLocalToRemoteCopy
    Tests to do:
      - Test multiple runs one after another:
           - confirm polling
      - Test multi-job checker
"""
import sys, os, getpass, shutil, time
import tempfile
import imp
import glob
import yaml
from pprint import pprint as pp
from os.path import join as pjoin
from os.path import dirname, basename, exists, isfile, isdir

import test_tools

USER = getpass.getuser()
verbose = False



class TestLocalToRemoteCopy(object):

    def setup_method(self, method):
        test_tools.remove_all_buffalofq_temp_dirs()
        self.source_data_dir = tempfile.mkdtemp(prefix='bfq_sd_')
        self.source_arc_dir  = tempfile.mkdtemp(prefix='bfq_sa_')
        self.dest_data_dir   = tempfile.mkdtemp(prefix='bfq_dd_')
        self.dest_link_dir   = tempfile.mkdtemp(prefix='bfq_dl_')
        self.feed_audit_dir  = tempfile.mkdtemp(prefix='bfq_fa_')
        self.config_dir      = tempfile.mkdtemp(prefix='bfq_c_')
        self.log_dir         = tempfile.mkdtemp(prefix='bfq_l_')
        self.pgm_path        = pjoin(dirname(os.path.split(os.path.abspath(__file__))[0]), 'buffalofq_mover')
        _make_file(self.source_data_dir,  'good')
        _make_file(self.source_data_dir,  'good')
        _make_file(self.source_data_dir,  'good')
        _make_file(self.source_data_dir,  'bad')
        _make_file(self.source_data_dir,  'bad')
        _make_file(self.source_arc_dir,   'ignore')
        _make_file(self.dest_data_dir,    'ignore')
        _make_file(self.dest_link_dir,    'ignore')

    def teardown_method(self, method):
        print
        print('audit results: ')
        os.system('cat %s' % pjoin(self.config_dir, 'buffalofq_audit.yml'))
        test_tools.remove_all_buffalofq_temp_dirs()

    def test_copy_many_files(self):
        """ Tests copying many files from source to dest
            AND leaving source files alone afterwards
            AND ignoring other files in all directories.
        """
        feed = _make_default_config(self.config_dir,
                                    self.log_dir,
                                    source_dir=self.source_data_dir,
                                    source_post_dir=self.source_arc_dir,
                                    dest_dir=self.dest_data_dir,
                                    source_post_action='pass')

        time.sleep(2) # must wait 2 seconds because config has polling dir of 1 sec
        #os.system('../buffalofq --configdir %s ' % self.config_dir)
        #os.system('../buffalofq --config-fqfn %s --console-log --log-level DEBUG' % pjoin(self.config_dir, 'buffalofq.yml'))
        os.system('%s --config-fqfn %s ' % (self.pgm_path, pjoin(self.config_dir, 'buffalofq.yml')))

        assert len(glob.glob(os.path.join(self.source_data_dir,'good*'))) == 3
        assert len(glob.glob(os.path.join(self.source_data_dir,'bad*')))  == 2

        assert len(glob.glob(os.path.join(self.source_arc_dir,'good*')))   == 0
        assert len(glob.glob(os.path.join(self.source_arc_dir,'bad*')))    == 0
        assert len(glob.glob(os.path.join(self.source_arc_dir,'ignore*'))) == 1
        assert len(glob.glob(os.path.join(self.dest_data_dir,'ignore*')))  == 1
        assert len(glob.glob(os.path.join(self.dest_link_dir,'ignore*')))  == 1

        assert len(glob.glob(os.path.join(self.dest_data_dir,'good*')))    == 3
        assert len(glob.glob(os.path.join(self.dest_data_dir,'bad*')))     == 0


    def test_copy_source_archival(self):
        """ Tests copying many files from source to dest
            AND archiving source files afterwards
            AND ignoring other files in all directories.
        """
        feed = _make_default_config(self.config_dir,
                                    self.log_dir,
                                    source_dir=self.source_data_dir,
                                    dest_dir=self.dest_data_dir,
                                    source_post_action='move',
                                    source_post_dir=self.source_arc_dir)

        time.sleep(2) # must wait 2 seconds because config has polling dir of 1 sec
        os.system('%s --config-fqfn %s ' % (self.pgm_path, pjoin(self.config_dir, 'buffalofq.yml')))

        assert len(glob.glob(os.path.join(self.source_data_dir,'good*'))) == 0
        assert len(glob.glob(os.path.join(self.source_data_dir,'bad*')))  == 2

        assert len(glob.glob(os.path.join(self.source_arc_dir,'good*')))   == 3
        assert len(glob.glob(os.path.join(self.source_arc_dir,'bad*')))    == 0
        assert len(glob.glob(os.path.join(self.source_arc_dir,'ignore*'))) == 1
        assert len(glob.glob(os.path.join(self.dest_data_dir,'ignore*')))  == 1
        assert len(glob.glob(os.path.join(self.dest_link_dir,'ignore*')))  == 1

        assert len(glob.glob(os.path.join(self.dest_data_dir,'good*')))    == 3
        assert len(glob.glob(os.path.join(self.dest_data_dir,'bad*')))     == 0


    def test_copy_source_delete(self):
        """ Tests copying many files from source to dest
            AND deleting source files afterwards
            AND ignoring other files in all directories.
        """
        feed = _make_default_config(self.config_dir,
                                    self.log_dir,
                                    source_dir=self.source_data_dir,
                                    dest_dir=self.dest_data_dir,
                                    source_post_action='delete')

        time.sleep(2) # must wait 2 seconds because config has polling dir of 1 sec
        os.system('%s --config-fqfn %s ' % (self.pgm_path, pjoin(self.config_dir, 'buffalofq.yml')))

        assert len(glob.glob(os.path.join(self.source_data_dir,'good*'))) == 0
        assert len(glob.glob(os.path.join(self.source_data_dir,'bad*')))  == 2

        assert len(glob.glob(os.path.join(self.source_arc_dir,'good*')))   == 0
        assert len(glob.glob(os.path.join(self.source_arc_dir,'bad*')))    == 0
        assert len(glob.glob(os.path.join(self.source_arc_dir,'ignore*'))) == 1
        assert len(glob.glob(os.path.join(self.dest_data_dir,'ignore*')))  == 1
        assert len(glob.glob(os.path.join(self.dest_link_dir,'ignore*')))  == 1

        assert len(glob.glob(os.path.join(self.dest_data_dir,'good*')))    == 3
        assert len(glob.glob(os.path.join(self.dest_data_dir,'bad*')))     == 0

        #os.system('ls -la %s' % self.config_dir)
        #os.system('cat %s' % os.path.join(self.config_dir, '*'))


    def test_copy_dest_symlink(self):
        """ Tests copying many files from source to dest
            AND deleting source files afterwards
            AND creating symlinks of destination files afterwards
            AND ignoring other files in all directories.
        """
        feed = _make_default_config(self.config_dir,
                                    self.log_dir,
                                    source_dir=self.source_data_dir,
                                    dest_dir=self.dest_data_dir,
                                    source_post_action='delete',
                                    dest_post_action='symlink',
                                    dest_post_action_symlink_dir=self.dest_link_dir,
                                    dest_post_action_symlink_fn=None)

        time.sleep(2) # must wait 2 seconds because config has polling dir of 1 sec
        os.system('%s --config-fqfn %s ' % (self.pgm_path, pjoin(self.config_dir, 'buffalofq.yml')))

        assert len(glob.glob(os.path.join(self.source_data_dir,'good*'))) == 0
        assert len(glob.glob(os.path.join(self.source_data_dir,'bad*')))  == 2

        assert len(glob.glob(os.path.join(self.source_arc_dir,'good*')))   == 0
        assert len(glob.glob(os.path.join(self.source_arc_dir,'bad*')))    == 0
        assert len(glob.glob(os.path.join(self.source_arc_dir,'ignore*'))) == 1
        assert len(glob.glob(os.path.join(self.dest_data_dir,'ignore*')))  == 1
        assert len(glob.glob(os.path.join(self.dest_link_dir,'ignore*')))  == 1

        assert len(glob.glob(os.path.join(self.dest_data_dir,'good*')))    == 3
        assert len(glob.glob(os.path.join(self.dest_data_dir,'bad*')))     == 0

        #print(glob.glob(os.path.join(self.dest_link_dir,'good*')))
        assert len(glob.glob(os.path.join(self.dest_link_dir,'good*')))    == 3



def _make_default_config(config_dir, log_dir, **kwargs):
    """ Inputs:
        - config_dir
        - kwargs - used to override config fields.  Note that
          source_dir and dest_dir must be provided.
    """
    feed = {}
    feed['name']               = 'source_2_dest'
    feed['status']             = 'enabled'
    feed['polling_seconds']    = 1
    feed['limit_total']        = 0
    feed['source_host']        = 'localhost'
    feed['source_user']        = USER
    feed['source_dir']         = None
    feed['source_fn']          = 'good*'
    feed['dest_host']          = 'localhost'
    feed['dest_user']          = USER
    feed['dest_dir']           = None
    feed['dest_fn']            = ''
    feed['port']               = 22
    feed['source_post_action'] = 'pass'
    feed['source_post_dir']    = ''
    feed['dest_post_action']   = 'pass'
    feed['log_dir']            = log_dir
    for key in kwargs.keys():
        feed[key] = kwargs[key]
    assert feed['source_dir']
    assert feed['dest_dir']

    #feed_list = []
    #feed_list.append(feed)
    #config_dict = {}
    #config_dict['feeds'] = feed_list
    #config_dict['log_level'] = 'DEBUG'
    config_dict = feed

    pp(config_dict)
    with open(os.path.join(config_dir, 'buffalofq.yml'), 'w') as config_fqfn:
        config_fqfn.write( yaml.dump(config_dict))
    ##os.system('cat %s' % os.path.join(config_dir, 'buffalofq.yml'))



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



