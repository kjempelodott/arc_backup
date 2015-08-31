"""
Module for job cancelling.
"""

from config import Config
from log import debug, error
from ssh import ssh_connect
from proc import *

def cancel(lrms, jobid):
     """
     Cancel job with ID ``jobid`` in the given batch system.

     :param str lrms: batch system (e.g. slurm)
     :param str jobid: local job ID in the batch system
     :param str remote: ``True`` if remote host
     :return: command return code
     :rtype: :py:obj:`int`
     """

     executable = None
     cmd = None
     if lrms == 'slurm':
          executable = 'scancel'
          cmd = '%s/s%s %s' % (Config.slurm_bin_path, executable, jobid)
     elif lrms == 'lsf':
          executable = 'bkill'
          cmd = '%s %s/%s -s 9 %s' % (Config.lsf_setup, Config.lsf_bin_path, executable, jobid)

     if Config.remote_host:
          ssh_connect(Config.remote_host, Config.remote_user, Config.private_key)

     debug('----- starting %sCancel.py -----' % lrms, 'common.cancel')
     debug('executing %s with job id %s' % (executable, jobid), 'common.cancel')
     execute = execute_local if not Config.remote_host else execute_remote
     handle = execute(cmd)
     rc = handle.returncode

     if rc:
          error('%s failed' % executable, 'common.cancel')
     error('----- exiting %sCancel.py -----' % lrms, 'common.cancel')
     return not rc
