"""
No batch system (fork) interface module.
"""

import os, sys, time, re
import arc
from common.config import Config, configure, is_conf_setter
from common.proc import execute_local, execute_remote
from common.log import debug, verbose, info, warn, error, ArcError
from common.scan import *
from common.ssh import ssh_connect
from common.submit import *


@is_conf_setter
def set_fork(cfg):
    """
    Set fork specific :py:data:`~lrms.common.Config` attributes.

    :param cfg: parsed arc.conf
    :type cfg: :py:class:`ConfigParser.ConfigParser`
    """
    Config.shared_filesystem = True

#-------------------
# Submit methods
#-------------------

def Submit(config, jobdesc):
    """
    Submits a job to the local or remote (SSH) machine. This method executes the required
    RunTimeEnvironment scripts and assembles the bash job script. The job script is
    written to file and run.

    :param str config: path to arc.conf
    :param jobdesc: job description object
    :type jobdesc: :py:class:`arc.JobDescription`
    :return: local job ID if successfully submitted, else ``None``
    :rtype: :py:obj:`str`
    """

    configure(config, set_fork)

    validate_attributes(jobdesc)
    if Config.remote_host:
        ssh_connect(Config.remote_host, Config.remote_user, Config.private_key)
  
    # Run RTE stage0
    debug('----- starting forkSubmitter.py -----', 'fork.Submit')
    RTE_stage0(jobdesc, 'fork')

    # Create tmp script file and write job script
    jobscript = get_job_script(jobdesc)
    script_file = write_script_file(jobscript)

    debug('Fork jobname: %s' % jobdesc.Identification.JobName, 'fork.Submit')
    debug('Fork job script built', 'fork.Submit')
    debug('----------------- BEGIN job script -----', 'fork.Submit')
    for line in jobscript.split('\n'):
        debug(line, 'fork.Submit')
    debug('----------------- END job script -----', 'fork.Submit')

    if 'ONLY_WRITE_JOBSCRIPT' in os.environ and os.environ['ONLY_WRITE_JOBSCRIPT'] == 'yes':
        return

    #######################################
    #  Submit the job
    ######################################
    
    execute = execute_local if not Config.remote_host else execute_remote
    directory = jobdesc.OtherAttributes['joboption;directory']

    debug('Session directory: %s' % directory, 'fork.Submit')

    handle = execute(script_file)

    if handle.returncode == 0:
        jobid = None
        try:
            jobid = handle.stdout[0][5:]
            debug('Job submitted successfully!', 'fork.Submit')
            debug('Local job id: ' + jobid, 'fork.Submit') 
            debug('----- exiting forkSubmitter.py -----', 'fork.Submit')
            return jobid
        except:
            pass

    debug('Job *NOT* submitted successfully!', 'fork.Submit')
    debug('Got error code: %d !' % (handle.returncode), 'fork.Submit')
    debug('Output is:\n' + ''.join(handle.stdout), 'fork.Submit')
    debug('Error output is:\n' + ''.join(handle.stderr), 'fork.Submit')
    debug('----- exiting forkSubmitter.py -----', 'fork.Submit')

                
def get_job_script(jobdesc):
    """
    Assemble bash job script.

    :param jobdesc: job description object
    :type jobdesc: :py:class:`arc.JobDescription`
    :return: job script
    :rtype: :py:obj:`str`
    """

    jobscript = \
        '#!/bin/sh\n' \
        '# Fork job script built by grid-manager\n' \
        '\n' \
        'echo pid: $$\n' # Print the pid (for SSH) \

    maxwalltime = 0
    if jobdesc.Resources.IndividualWallTime.range.max > 0:
        maxwalltime = jobdesc.Resources.IndividualWallTime.range.max
    elif jobdesc.Resources.TotalCPUTime.range.max > 0:
        maxwalltime = jobdesc.Resources.TotalCPUTime.range.max
    if maxwalltime:
        jobscript += 'ulimit -t %d\n' % maxwalltime

    jobscript += JobscriptAssembler(jobdesc).get_standard_jobscript()
    return jobscript


#-------------------
# Cancel methods
#-------------------

def Cancel(config, jobid):
    """
    Cancel a job. The TERM signal is sent to allow the process to terminate
    gracefully within 5 seconds, followed by a KILL signal.

    :param str config: path to arc.conf
    :param str jobid: local job ID
    :return: ``True`` if successfully cancelled, else ``False``
    :rtype: :py:obj:`bool`
    """

    debug('----- starting forkCancel.py -----', 'fork.Cancel')

    configure(config)
    if Config.remote_host:
        ssh_connect(Config.remote_host, Config.remote_user, Config.private_key)

    info('Killing job with pid %s' % jobid, 'fork.Cancel')
    if not Config.remote_host:
        import signal
        try:
            os.kill(jobid, signal.SIGTERM)
            time.sleep(5)
            os.kill(jobid, signal.SIGKILL)
        except OSError:
            # Job already died or terminated gracefully after SIGTERM
            pass
        except:
            return False
    else:
        args = 'kill -s TERM %i; sleep 5; kill -s KILL %i' % (jobid, jobid)
        handle = execute_remote(args)

    debug('----- exiting forkCancel.py -----', 'fork.Cancel')
    return True


#---------------------
# Scan methods
#---------------------

def Scan(config, ctr_dirs):
    """
    Query the local or remote (SSH) machine for all jobs in /[controldir]/processing.
    If the job has stopped running, the exit code is read and the comments file
    is updated.

    :param str config: path to arc.conf
    :param ctr_dirs: list of paths to control directories 
    :type ctr_dirs: :py:obj:`list` [ :py:obj:`str` ... ]
    """

    configure(config, set_fork)
    if Config.scanscriptlog:
        scanlogfile = arc.common.LogFile(Config.scanscriptlog)
        arc.common.Logger_getRootLogger().addDestination(scanlogfile)
        arc.common.Logger_getRootLogger().setThreshold(Config.log_threshold)

    jobs = get_jobs(ctr_dirs)
    if not jobs: return
    if Config.remote_host:
        ssh_connect(Config.remote_host, Config.remote_user, Config.private_key)

    execute = execute_local if not Config.remote_host else execute_remote
    args = 'ps -opid ' + (' '.join(jobs.keys()))
    if os.environ.has_key('__FORK_TEST'):
        handle = execute(args, env=dict(os.environ))
    else:
        handle = execute(args)
    if handle.returncode != 0:
        debug('Got error code %i from ps -opid' % handle.returncode, 'fork.Scan')
        debug('Error output is:\n' + ''.join(handle.stderr), 'fork.Scan')

    running = [line.strip() for line in handle.stdout]
    for localid, job in jobs.items():
        if localid in running:
            continue
        if set_exit_code_from_diag(job):
            job.message = MESSAGES[job.state]
        else:
            job.exitcode = -1
        
        with open(job.lrms_done_file, 'w') as f:
            f.write('%i %s\n' % (job.exitcode, job.message))
        write_comments(job)
