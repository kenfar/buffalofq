#!/usr/bin/env python

import os, sys, time
import errno
import logging
import os.path
from os.path import dirname, basename, exists, isdir, isfile, join as pjoin

import fnmatch
import paramiko
from pprint import pprint as pp

#--- our modules -------------------
import bfq_auditor


FAIL_STEP    = -1     # used by test-harness to force fails, -1 == no fail
FAIL_SUBSTEP = -1     # used by test-harness to force fails, -1 == no fail
FAIL_CATCH   = False  # used by test-harness to force fails
logger       = None   # will get set to logging api later



class HandleOneFeed(object):

    def __init__(self,
                 feed,
                 audit_dir,
                 limit_total,
                 config_name,
                 key_filename):

        self.feed            = feed
        self.auditor         = bfq_auditor.FeedAuditor(self.feed['name'], audit_dir, config_name=config_name)
        self.limit_total     = limit_total
        self.state_good      = None
        self.poll_good       = None  # true if it is time to poll again
        self.poll_last_time  = 0
        self.files           = None
        self.transport       = None
        self.sftp            = None
        self.key_filename    = key_filename
        self.mykey           = None
        self.file_cnt        = 0


    def file_check(self, force=False):
        self._check_prereqs()
        if self.poll_good or force:
            self.files = self._get_files_to_move()
            self.mykey = self._get_key()
            (self.transport, self.sftp) = self._setup_connection()

    def _check_prereqs(self):
        """ Checks all feed prerequisites:
            - recovery state
            - polling duration
        """
        # todo: confirm that both source & dest have sufficient disk space
        self.state_good = self._check_state()
        if self.auditor.status['empty_audit']:
            self.poll_good  = self._check_polling(self.poll_last_time, self.feed['polling_seconds'])
        else:
            self.poll_good  = self._check_polling(self.auditor.status['time'], self.feed['polling_seconds'])

    def close(self):
        if self.sftp:
            self.sftp.close()
            self.transport.close()


    def run(self, force, suppcheck=None):

        processed_last_time = 0
        logger.info('HandleOneFeeds run starting')

        while True:

            if suppcheck:  # suppcheck will be None for testing
                if suppcheck.suppressed(self.feed):
                    logger.warning('feed has been suppressed - will terminate')
                    break

            self.file_check(force)

            if self.poll_good:
                if self.files:
                    self.do_all_files()
                processed_last_time = time.time()
            elif force:
                if self.files:
                    logger.info('Insufficient Polling Duration - will force anyway')
                    self.do_all_files()
                processed_last_time = time.time()
            else:
                # after 5 minutes of polling write a log message:
                if (time.time() - processed_last_time) > 300:
                    logger.info('polling continuing')
                    processed_last_time = time.time()
                # can safely sleep for a few:
                time.sleep(self.feed['polling_seconds'])

            # Quit if not running continuously:
            if self.limit_total > -1:
                break

        self.close()


    def do_all_files(self):
        """ if any tasks for any files fail - skip all remaining
            processing.
        """
        # todo: should probably log if not self.sftp...
        if self.sftp:
            for one_file in self.files:
                handle_one_file = HandleOneFile(self.feed,
                                                one_file,
                                                self.auditor,
                                                self.sftp)
                if not handle_one_file.run_all_steps():
                    break
                self.file_cnt += 1
                if self.limit_total > 0 and self.file_cnt >= self.limit_total:
                    logger.debug('limit_total reached, file movement stopped')
                    break


    def _check_state(self):

        if (self.auditor.status['step']    == 6
            and self.auditor.status['status']  == 'stop'
            and self.auditor.status['result']  == 'pass'):
            return True  # last run completed successfully
        elif (self.auditor.status['step']  == 0
            and self.auditor.status['status']  == 'stop'
            and self.auditor.status['result']  == 'pass'):
            return True  # this is the first run
        else:
            return False # last run in progress or failed


    def _check_polling(self, poll_last_time, poll_required_dur):
        poll_actual_dur = time.time() - poll_last_time

        if poll_actual_dur >  poll_required_dur:
            return True
        else:
            return False


    def _get_files_to_move(self):
        """ Gets a list of qualifying files to be sent from the local
            file system.  In a recovery scenario it only gets the file
            name that previously failed.
        """
        # todo: need to support both pushing & pulling files
        step = 0
        self.poll_last_time = time.time()
        # shouldn't this just check for recovery_mode?!?
        if (self.auditor.status['fn']
            and (not self.state_good
                 or not good_to_run(step, self.auditor.status))):
            return [self.auditor.status['fn']]
        else:
            self.auditor.write(step=step, status='start', fn='')
            source_files   = os.listdir(self.feed['source_dir'])
            #logger.debug('files found: %d' % len(source_files))
            filtered_files = fnmatch.filter(source_files,
                                            self.feed['source_fn'])
            #logger.debug('files found - after filtering: %d' % len(filtered_files))
            sorted_filtered_files = self._sort_files(filtered_files)
            #logger.debug('files found - after sorting: %d' % len(sorted_filtered_files))
            fail_check(step)
            self.auditor.write(step=step, status='stop', result='pass')
            return sorted_filtered_files


    def _sort_files(self, files):
        """ Uses self.feed['sort_key']:
                - None = no sort
                - name = alphabetic on name
                - 'field:?' = looks for this key in filename, sorts by associated value
        """
        if self.feed['sort_key'] is None:
            return files
        elif self.feed['sort_key'] == 'name':
            return sorted(files)
        elif self.feed['sort_key'].startswith('field:'):
            parts = self.feed['sort_key'].split(':')
            return sort_files_by_fields(files, parts[1])
        else:
            msg = 'Invalid sort_key: %s' % self.feed['sort_key']
            logger.critical(msg)
            raise ValueError(msg)


    def _get_key(self):
        # todo: allow user to specify key to use via config file
        # private key must not be encrypted (must not have a passphrase):
        pkfile = os.path.expanduser('~/.ssh/%s' % self.key_filename)
        return paramiko.RSAKey.from_private_key_file(pkfile)


    def _setup_connection(self):

        transport = paramiko.Transport((self.feed['dest_host'],
                                             self.feed['port']))

        transport.connect(username=self.feed['dest_user'],
                          pkey=self.mykey)
        sftp = paramiko.SFTPClient.from_transport(transport)
        return transport, sftp





class HandleOneFile(object):

    def __init__(self, feed, one_file, auditor, sftp):
        self.feed           = feed
        self.file           = one_file
        self.auditor        = auditor
        self.sftp           = sftp
        self.source_fqfn    = os.path.join(self.feed['source_dir'], self.file)
        self.dest_fqfn      = os.path.join(self.feed['dest_dir'], self.file)
        self.dest_temp_fqfn = '%s.temp' % self.dest_fqfn
        logger.info('HandleOneFile starting on file: %s' % one_file)


    def run_all_steps(self):
        """ Handle all processing for a single file
            Returns True or False: False if any task fails.
        """

        if self._step_runner(1, self._do_source_pre_actions) is False:
            return False

        if self._step_runner(2, self._do_dest_pre_actions) is False:
            return False

        if self._step_runner(3, self._copy_file) is False:
            return False

        if self._step_runner(4, self._rename_dest_file) is False:
            return False

        if self._step_runner(5, self._do_dest_post_actions) is False:
            return False

        if self._step_runner(6, self._do_source_post_actions) is False:
            return False

        return True


    def _step_runner(self, step, task):
        """ Runs a single step and returns:
            - True - indicates the process succeeded
            - False - indicates the process failed, or has been forced to fail
              by the test-harness.
           Note that most of this code is overhead - validation, auditing, and
           enabling the test-harness to force failures.
        """
        result = None
        fail_check(step, substep='a')
        if good_to_run(step, self.auditor.status):
            fail_check(step, substep='b')
            self.auditor.write(step=step, status='start', fn=self.file)
            fail_check(step, substep='c')

            # run main task
            result = task()
            assert result is not None
            if fail_check(step, substep='d'):
                result = False

            assert(result is not None)
            self.auditor.write(step=step, status='stop', result=result)
            fail_check(step, substep='e')
        else:
            logger.info('HandleOneFile._step_runner: step was bypassed: %d' % step)

        assert result in [None, True, False]
        return result


    def _do_source_pre_actions(self):
        # rename file
        # move file
        return True


    def _do_dest_pre_actions(self):
        # check space
        # check for dups?
        # create directory?
        # check for file differences?
        return True


    def _copy_file(self):
        self.sftp.put(self.source_fqfn, self.dest_temp_fqfn)
        return True


    def _rename_dest_file(self):
        try:
            self.sftp.rename(self.dest_temp_fqfn, self.dest_fqfn)
        except IOError, e:
            #print 'errno:          %d' % e.errno
            #print 'file:           %s' % file
            #print 'dest_fqfn:      %s' % dest_fqfn
            #print 'dest_temp_fqfn: %s' % dest_temp_fqfn
            #print 'dest dir: '
            #pp(glob.glob(os.path.join(self.feed['dest_dir'],'*')))
            logger.error('_rename_dest_file got IOError - will remove dest and repeat rename')
            self.sftp.remove(self.dest_fqfn)
            self.sftp.rename(self.dest_temp_fqfn, self.dest_fqfn)
        return True



    def _do_dest_post_actions(self):
        # remove temp dir?
        # change privs?
        # run crc check?
        if self.feed.get('dest_post_action', 'unk') == 'crccheck':
            logger.error('dest_post_actions of crccheck not yet implemented')
            raise ValueError
        elif self.feed.get('dest_post_action_symlink_dir'):
            return task_make_dest_symlink(self.sftp,
                            self.feed['dest_dir'],
                            self.file,
                            self.feed['dest_post_action_symlink_dir'],
                            self.feed['dest_post_action_symlink_fn'])
        else:
            return True



    def _do_source_post_actions(self):
        # rename file
        if self.feed.get('source_post_action', 'unk') == 'delete':
            return task_delete_source_file(self.source_fqfn)
        elif self.feed.get('source_post_action', 'unk') == 'move':
            return task_move_source_file(self.source_fqfn,
                           pjoin(self.feed['source_post_dir'], self.file))
        else:
            return True



def task_delete_source_file(source_fqfn):
    try:
        os.remove(source_fqfn)
    except IOError as e:
        if e.errno == errno.ENOENT:
            return True
        else:
            return False
    except:
        return False
    else:
        return True



def task_move_source_file(old_fqfn, new_fqfn):
    try:
        os.rename(old_fqfn, new_fqfn)
    except OSError as e:
        if (e.errno == errno.ENOENT
        and os.path.exists(new_fqfn)):
            return True
        else:
            return False
    except:
        return False
    else:
        return True



def task_make_dest_symlink(sftp, source_dir, source_fn, dest_dir, dest_fn=None):
    """ Creates a symbolic link
        - note - needs new process for removing old symbolic links.
    """
    if not dest_fn:
       dest_fn = source_fn

    source_fqfn  = os.path.join(source_dir, source_fn)
    symlink_fqfn = os.path.join(dest_dir, dest_fn)
    try:
        sftp.remove(symlink_fqfn)
    except IOError:
        pass # could be non-existing file which is ok
    sftp.symlink(source_fqfn, symlink_fqfn)
    return True



def fail_check(step, substep=None):
    """ Inputs:
            - step: value will be compared to global variable to determine
              whether or not to force a failure. Should mark a large segment
              of process.
            - substep: value will be compared to global variable to determine
              whether or not to force a failure.  Should mark a small part of
              a step.
        Results:
            - if step and substep match, then force a failure.
            - if catch if True, function will return True for caller to handle.
              otherwise, this function will run sys.exit
    """
    if step == FAIL_STEP:
        if (substep == FAIL_SUBSTEP or substep is None):
            if FAIL_CATCH:
                return True
            else:
                sys.exit(0)

    return False




def good_to_run(new_step, old_status):
    assert old_status['result'] in ['fail', 'pass', 'tbd']

    steps = {}
    steps[0] = {'recovery_step':0, 'prior_steps':[0, 6]}
    steps[1] = {'recovery_step':1, 'prior_steps':[0, 6]}
    steps[2] = {'recovery_step':2, 'prior_steps':[1]}
    steps[3] = {'recovery_step':3, 'prior_steps':[2]}
    steps[4] = {'recovery_step':3, 'prior_steps':[3]}
    steps[5] = {'recovery_step':5, 'prior_steps':[4]}
    steps[6] = {'recovery_step':6, 'prior_steps':[5]}

    if old_status['result'] in ['fail','tbd']:
        if new_step == steps[old_status['step']]['recovery_step']:
            return True
        else:
            return False
    else:
        if old_status['step'] in steps[new_step]['prior_steps']:
            return True
        else:
            return False


def setup_logging(log_name):
    global logger
    logger = logging.getLogger(log_name + '.buffguts')


def sort_files_by_fields(files, *keys):
    def fn_sort(fn):
        vals = []
        for key in keys:
           vals.append(filename_field_get(fn, key))
        return  tuple(vals)
    files.sort(key=fn_sort)
    return files

def filename_field_get(filename, key):
    assert filename is not None
    assert key is not None

    segments = basename(filename).split('.')
    kv_pairs = segments[0].split('_')
    for kv in kv_pairs:
        kv_parts = kv.split('-')
        if len(kv_parts) == 2:
            if kv_parts[0] == key:
                return kv_parts[1]

