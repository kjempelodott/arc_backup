"""
Module for job cancelling.
"""

from config import Config
from ssh import ssh_connect
from proc import *

def cancel(lrms, job_id):
     """
     Cancel job with ID ``job_id`` within the given batch system.

     :param str lrms: batch system (e.g. slurm)
     :param str job_id: local job ID within the batch system
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

     log(arc.DEBUG, '----- starting %sCancel.py -----' % lrms, 'common.cancel')
     log(arc.DEBUG, 'executing %s with job id %s' % (executable, job_id), 'common.cancel')
     execute = excute_local if not Config.remote_host else execute_remote
     handle = p.execute(cmd)
     rc = handle.returncode

     if rc:
          log(arc.ERROR, '%s failed' % executable, 'common.cancel')
     log(arc.DEBUG, '----- exiting %sCancel.py -----' % lrms, 'common.cancel')
     return not rc
