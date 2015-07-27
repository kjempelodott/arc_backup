"""
Defines constants and functions related to scanning.

RUNNING: list of possible states reported by the bactch system for jobs that are running
MESSAGES: mapping between job state reported by batch system and grid-manager state
UID: user ID of user running PythonLRMS
GID: group ID of user running PythonLRMS
"""

import os
from config import Config
from files import read, write, getmtime
from proc import execute_local, execute_remote


RUNNING  = ['PENDING','RUNNING','SUSPENDED','COMPLETING',     # slurm
            'PSUSP','USUSP','SUSSP','RUN','PEND',             # lsf
            'B', 'E', 'H', 'M', 'Q', 'R', 'S', 'T', 'U', 'W', # pbs
            '1', '2', '4', '8', '16', '17', '18']             # sceapi
UID      = os.getuid()
GID      = os.getgid()
MESSAGES = {
    'UNKNOWN'  :'',
    'COMPLETED':'Job completed',
    'FAILED'   :'Job failed',
    # slurm
    'CANCELLED':'Job was cancelled',
    'TIMEOUT'  :'Job timeout',
    'NODE_FAIL':'Node fail',
    # lsf
    'DONE'     :'Job completed',
    'EXIT'     :'Job failed',
    # pbs
    'F'        :'Job completed',
    # sceapi
    '20'       :'Job completed',
    '24'       :'Job failed',
    '32'       :'Job was cancelled',
    '33'       :'Net delay',    
    '34'       :'Subjob error',
    '38'       :'Exit',
    }


def get_jobs(ctrdirs):
    """
    Scan control directories for jobs with status *INLRMS* or *CANCELING*.

    :param list ctrdirs: list of paths to control directories
    :return: dictionary that maps local job ID to job object
    :rtype: :py:obj:`dict` { :py:obj:`str` : :py:obj:`object` ... }

    .. note:: The returned job obects have the following attributes: 
    ``localid``, ``gridid``, ``local_file``, ``lrms_done_file``, ``grami_file``,
    ``output_file``, ``state``, ``uid``, ``gid``, ``sessiondir``,
    ``diag_file``, ``count_file``, ``errors_file`` and ``comment_file``.
    """

    jobs = {}
    for ctrdir in ctrdirs:
        procdir = ctrdir + '/processing'
        for fname in os.listdir(procdir):
            try:
                globalid = re.search(r'job.(?P<id>\w+).status',fname).groupdict()['id']
                with open('%s/%s' % (procdir, fname), 'r') as f:
                    if re.search('INLRMS|CANCELING', f.readline()):
                        job = Object()
                        job.globalid = globalid
                        job.controldir = ctrdir
                        job.local_file = '%s/job.%s.local' % (ctrdir, job.globalid)
                        if not read_local_file(job): # sets localid and sessiondir
                            continue
                        job.lrms_done_file = '%s/job.%s.lrms_done' % (ctrdir, job.globalid)
                        job.grami_file = '%s/job.%s.grami' % (ctrdir, job.globalid)
                        job.output_file = '%s/job.%s.output' % (ctrdir, job.globalid)
                        job.count_file = '%s/job.%s.lrms_job' % (ctrdir, job.globalid)
                        job.state = 'UNKNOWN'
                        job.message = ''
                        job.diag_file = '%s.diag' % job.sessiondir
                        job.errors_file = '%s.errors' % job.sessiondir
                        job.comment_file = '%s.comment' % job.sessiondir
                        jobs[job.localid] = job
                        try:
                            job.uid = os.stat(job.diag_file).st_uid
                            job.gid = os.stat(job.diag_file).st_gid
                        except:
                            verbose('Failed to stat %s' % job.diag_file, 'common.scan')
                            job.uid = UID
                            job.gid = GID
            except AttributeError:
                # Not a jobfile
                continue
            except IOError as e:
                # Possibly .status file deleted by other process
                verbose('IOError when scanning for jobs in /processing:\n%s' % str(e))
                continue
    return jobs


def set_exit_code_from_diag(job):
    """
    Retrieve exit code from the diag file and set the ``job.exitcode`` attribute.

    :param job: job object 
    :type job: :py:obj:`object`
    :return: ``True`` if exit code was found, else ``False`` 
    :rtype: :py:obj:`bool`
    """

    # In case of non-NFS setup it may take some time till
    # diagnostics file is delivered. Wait for it max 2 minutes.
    time_to_wait = 10 if Config.shared_filesystem else 120
    time_slept = 0
    time_step = 0.5

    previous_mtime = current_mtime = getmtime(job.diag_file)
    while True:
        content = read(job.diag_file, 1000)
        for line in content:
            if line[:9] == 'exitcode=':
                job.exitcode = int(line[9:])
                job.state = 'COMPLETED' if job.exitcode == 0 else 'FAILED'
                return True
        previous_mtime = current_mtime
        current_mtime = getmtime(job.diag_file)
        # Non-successful read, but mtime changed. Reload file.
        if current_mtime > previous_mtime:
            # Possibly infinite loop?
            continue
        # Wait
        if time_slept >= time_to_wait:
            warn('Failed to get exit code from diag file', 'common.scan')
            return False
        time.sleep(time_step)
        time_slept += time_step

    warn('Failed to get exit code from diag file', 'common.scan')
    return False


def add_failure(job):
    """
    Add +1 to the failure counter. If a job has more than 5 failures,
    it is marked as lost with unknown exit code. A failure is typically
    added if the job stopped running, but reading exit code from diag 
    file failed.
    """
    with open(job.count_file, 'a+') as cf:
        print >> cf, \
            int(cf.read() if cf.read() and (cf.seek(0) or True)
                else 0) + (cf.seek(0) or cf.truncate() or 1)
    fail_count = int('0' + open(job.count_file, 'r').read())
    if fail_count >= 6:
        os.remove(job.count_file)
        job.exitcode = -1
        job.message = 'Job was lost with unknown exit code. Status: ' + MESSAGES[job.state]

 
def update_diag(job):
    """
    Filters out WallTime from the diag file if present and
    replaces it with output from the batch system. It also adds 
    StartTime and EndTime for accounting.

    :param job: job object
    :type job: :py:obj:`object`
    """

    content = read(job.diag_file)
    diag_dict = {}
    for line in content:
        line = line.strip(' \n#')
        if not line:
            continue
        key, value = line.split('=', 1)
        if key[:9] == 'frontend_':
            continue
        if key not in diag_dict:
            diag_dict[key] = []
        diag_dict[key].append(value)

    # Do not save the 'frontend_*' and 'ExecutionUnits' keys,  
    # they are set on the font-end. Not to be overwritten
    diag_dict.pop('ExecutionUnits', None)
    
    keys = ['nodename', 'WallTime', 'UserTime', 'KernelTime', 
            'AverageTotalMemory', 'AverageResidentMemory', 'exitcode',
            'LRMSStartTime', 'LRMSEndTime', 'LRMSExitcode',
            'LRMSMessage']
    for key in keys:
        if key in diag_dict:
            diag_dict[key] = diag_dict[key][-1:]

    if hasattr(job, 'Processors'):
        diag_dict['Processors'] = [job.Processors]
    if hasattr(job, 'LRMSStartTime'):
        diag_dict['LRMSStartTime'] = [job.LRMSStartTime.str(arc.common.MDSTime)]
    if hasattr(job, 'LRMSEndTime'):
        diag_dict['LRMSEndTime'] = [job.LRMSEndTime.str(arc.common.MDSTime)]
    if hasattr(job, 'WallTime'):
        diag_dict['WallTime'] = ['%ds' % (job.WallTime.GetPeriod())]

    if not diag_dict.has_key('exitcode') and hasattr(job, 'exitcode'):
        diag_dict['exitcode'] = [job.exitcode]

    buf = ''
    for k, vs in diag_dict.iteritems():
        buf += '\n'.join('%s=%s' % (k, v) for v in vs) + '\n'
    if write(job.diag_file, buf, 0644):
        # Set job user as owner
        os.chown(job.diag_file, job.uid, job.gid)


def read_local_file(job):
    """
    Read the local file and set  ``job.localid`` and ``job.sessiondir`` attributes.

    :param job: job object
    :type job: :py:obj:`object`
    :return: ``True`` if successful, else ``False``
    :rtype: :py:obj:`bool`
    """

    try:
        content = dict(item.split('=', 1) for item in read(job.local_file))
        job.localid = content['localid'].strip()
        job.sessiondir = content['sessiondir'].strip()
        return True
    except Exception as e: 
        error('Failed to get local ID or sessiondir from local file (%s)' % globalid, 'common.scan')
        return False


def gm_kick(jobs):
    """
    Execute ``gm-kick``.

    :param job: list of jobs to be kicked
    :type job: :py:obj:`list` [ :py:obj:`object` ... ]
    """    

    # Execute locally.
    job_local_files = [j.local_file for j in jobs]
    libexecdir = '%s/libexec/arc' % (os.environ['ARC_LOCATION']) \
        if 'ARC_LOCATION' in os.environ else ''
    execute_local('%s/gm-kick %s' % (libexecdir, ' '.join(job_local_files)))


def write_comments(job):
    """
    Write content of comment file to errors file.

    :param job: job object
    :type job: :py:obj:`object`
    """

    comments = read(job.comment_file)
    if comments:
        buf = \
        '------- '
        'Contents of output stream forwarded by the LRMS '
        '---------\n'
        buf += '\n'.join(comments)
        buf += \
        '------------------------- '
        'End of output '
        '-------------------------'
        write(job.errors_file, buf, 0644, True)
 

def get_MDS(dm, lc_time = 'en_US'):
    """
    Get date and time in MDS format.
    
    :param dm: dictionary with keys 'yyyy', 'mm'|'bbb',
    'dd', 'HH', 'MM' and 'SS'
    :type dm: :py:obj:`dict`
    :param str lc_time: the locale, if 'bbb' given
    :return: string on the form 'yyyy-mm-ddTHH:MM:SS'
    :rtype: :py:obj:`str`
    """

    now = arc.common.Time().str(arc.common.MDSTime)

    if 'bbb' in dm and 'mm' not in dm:
        try:
            import locale
            locale.setlocale(locale.LC_TIME, lc_time)
            from datetime import datetime
            dm['mm'] = datetime.strptime(dm['bbb'],'%b').month
        except:
            pass
            
    return '%s-%s-%sT%s:%s:%s' % (dm['yyyy'] if 'yyyy' in dm else now[:4], dm['mm'] if 'mm' in dm else now[4:6], 
                                  dm['dd'], dm['HH'], dm['MM'], dm['SS'] if 'SS' in dm else '00')


def is_running(pid):
    """
    Check if process is running on the local machine.
    
    :param int pid: process ID
    :return: ``True`` if process is running, else ``False``
    :rtype: :py:obj:`bool`
    """

    return pid in os.listdir('/proc')
