"""
Common job cancel functions.
"""

from common import *
from proc import *

def cancel(cmd, job_id, lrms, remote = False):
     """
     Cancel job (applicable for all LRMSs except fork and boinc).

     :param str cmd: cancel command (e.g. '/usr/bin/scancel')
     :param str job_id: job ID within the LRMS
     :param str lrms: name of the LRMS
     :param str remote: ``True`` if remote host
     :return: command return code
     :rtype: :py:obj:`int`
     """

     executable = os.path.basename(cmd.split()[0])
     
     log(arc.DEBUG, '----- starting %sCancel.py -----' % lrms, 'common.cancel')
     log(arc.DEBUG, 'executing %s with job id %s' % (executable,job_id), 'common.cancel')
     execute = excute_local if not remote else execute_remote
     handle = p.execute(cmd)
     rc = handle.returncode

     if rc:
          log(arc.ERROR, '%s failed' % executable, 'common.cancel')
     log(arc.DEBUG, '----- exiting %sCancel.py -----' % lrms, 'common.cancel')
     return not rc


def get_status_file(ctrdir, gridid):
     """
     Get the job status file (for fork and boinc).

     :param str ctrdir: path to GRID control directory
     :param str gridid: GRID (global) job ID
     :return: path to job status file
     :rtype: :py:obj:`str`
     """

     status_file = 'job.%s.status' % gridid
     paths = (os.path.join(ctrdir, 'accepting', status_file),
              os.path.join(ctrdir, 'processing', status_file),
              os.path.join(ctrdir, 'finished', status_file))
     for p in paths:
          if os.access(p, R_OK): return p
     raise ArcError('Status file for job %s not found in \'%s\'. '
                    'Job was not killed, if running at all.' % (gridid, ctrdir), 'common.cancel')
