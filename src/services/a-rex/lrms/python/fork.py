"""
Fork batch system interface module.
"""

import os, sys, time, re
import arc
from common import UserConfig
from common.config import *
from common.proc import * 
from common.parse import SimpleGramiParser
from common.scan import *
from common.submit import *


@is_conf_setter
def set_fork(cfg):
    """
    Set fork specific options in :py:data:`~lrms.common.common.Config`.

    :param cfg: parsed arc.conf
    :type cfg: :py:class:`ConfigParser.ConfigParser`
    """
    Config.shared_filesystem = True

#-------------------
# Submit methods
#-------------------

def Submit(config, jobdescs, jc):
    """
    Submits a job to the host specified in arc.conf. This method executes the required
    Run Time Environment scripts and assembles the bash job script. The job script is
    written to file and submitted to the specified fork queue.

    :param str config: path to arc.conf
    :param jobdescs: job description list object
    :type jobdescs: :py:class:`arc.JobDescriptionList`
    :param jc: job container object 
    :type jc: :py:class:`arc.compute.JobContainer`
    :return: ``True`` if successfully submitted, else ``False``
    :rtype: :py:obj:`bool`
    """

    configure(config, set_fork)

    jd = jobdescs[0]
    validate_attributes(jd)
  
    # Run RTE stage0
    debug('----- starting forkSubmitter.py -----', 'fork.Submit')
    RTE_stage0(jobdesc)

    # Create tmp script file and write job script
    jobscript = get_job_script(jobdescs)
    script_file = write_script_file(jobscript)

    debug('Fork jobname: %s' % jd.Identification.JobName, 'fork.Submit')
    debug('Fork job script built', 'fork.Submit')
    debug('----------------- BEGIN job script -----', 'fork.Submit')
    for line in jobscript.split('\n'):
        debug(line, 'fork.Submit')
    debug('----------------- END job script -----', 'fork.Submit')

    if 'ONLY_WRITE_JOBSCRIPT' in os.environ and os.environ['ONLY_WRITE_JOBSCRIPT'] == 'yes':
        return False

    #######################################
    #  Submit the job
    ######################################
    
    execute = excute_local if not Config.remote_host else execute_remote
    directory = jd.OtherAttributes['joboption;directory']

    debug('Session directory: %s' % directory, 'fork.Submit')

    handle = execute(script_file)
    jobid = handle.stdout[0][5:]

    # Write output to comment file
    with open(directory + '.comment', 'w') as out:
        for line in fork_handle.stdout:
            out.write(line)
        for line in fork_handle.stderr:
            out.write(line)

    if handle.returncode == 0:
        debug('Job submitted successfully!', 'fork.Submit')
        debug('Local job id: %s' % (jobid), 'fork.Submit') 
        debug('----- exiting forkSubmitter.py -----', 'fork.Submit')

        endpointURL = arc.common.URL(Config.remote_endpoint)
        newJob = arc.Job()
        newJob.JobID = endpointURL.Host() + ':' + jobid if endpointURL.Host() else jobid
        # TODO: What interface name to use?
        newJob.ServiceInformationInterfaceName = 'org.nordugrid.slurm.sbatch'
        newJob.JobStatusInterfaceName = 'org.nordugrid.slurm.sbatch'
        newJob.JobManagementInterfaceName = 'org.nordugrid.slurm.sbatch'
        # TODO: Change returned endpoints for job. 
        # Currently these URLs are not usable.
        newJob.ServiceInformationURL = arc.common.URL('test://localhost')
        newJob.JobStatusURL = endpointURL
        newJob.JobManagementURL = endpointURL
        newJob.SessionDir  = arc.common.URL('file://' + directory)
        newJob.StageInDir  = newJob.SessionDir
        newJob.StageOutDir = newJob.SessionDir
        newJob.IDFromEndpoint = jobid
        jc.addEntity(newJob)
        return True

    debug('Job *NOT* submitted successfully!', 'fork.Submit')
    debug('Got error code: %d !' % (handle.returncode), 'fork.Submit')
    debug('Output is:', 'fork.Submit')
    debug('\n'.join(handle.stdout), 'fork.Submit')
    debug('Error output is:', 'fork.Submit')
    debug('\n'.join(handle.stderr), 'fork.Submit')
    debug('----- exiting forkSubmitter.py -----', 'fork.Submit')
    return False

                
def get_job_script(jobdescs):
    """
    Assemble bash job script for a fork host.

    :param jobdescs: list of job description objects
    :type jd: :py:obj:`list` [ :py:class:`arc.JobDescription` ... ]
    :return: job script
    :rtype: :py:obj:`str`
    """

    jobscript = \
        '#!/bin/sh\n' \
        '# Fork job script built by grid-manager\n' \
        '\n' \
        'echo pid: $$\n' # Print the pid (for SSH) \

    maxwalltime = 0
    if jobdescs[0].Resources.IndividualWallTime.range.max > 0:
        maxwalltime = jobdescs[0].Resources.IndividualWallTime.range.max
    elif jobdescs[0].Resources.TotalCPUTime.range.max > 0:
        maxwalltime = jobdescs[0].Resources.TotalCPUTime.range.max
    if maxwalltime:
        jobscript += 'ulimit -t %d\n' % maxwalltime

    jobscript += \
        '# source with arguments for DASH shells\n' \
        'sourcewithargs() {\n' \
        'script=$1\n' \
        'shift\n' \
        '. $script\n' \
        '}\n\n' \
        '# Overide umask of execution node ' \
        '(sometime values are really strange)\n' \
        'umask 077\n'

    jobscript += add_user_env(jobdescs)
    if Config.localtransfer:
        jobscript += setup_local_transfer(jobdescs)
    jobscript += setup_runtime_env(jobdescs)
    jobscript += move_files_to_node(jobdescs)
    jobscript += '\nRESULT=0\n\n'
    if Config.localtransfer:
        jobscript += download_input_files(jobdescs)
    jobscript += '\n'
    jobscript += 'if [ "$RESULT" = \'0\' ] ; then\n'
    jobscript += RTE_stage1(jobdescs)
    jobscript += 'echo "runtimeenvironments=$runtimeenvironments" >> "$RUNTIME_JOB_DIAG"\n'
    jobscript += cd_and_run(jobdescs)
    jobscript += 'fi\n'
    if Config.localtransfer:
        jobscript += upload_output_files(jobdescs)
    jobscript += '\n'
    jobscript += configure_runtime(jobdescs)
    jobscript += move_files_to_frontend()
    return jobscript


#-------------------
# Cancel methods
#-------------------

def Cancel(config, grami_file):
    """
    Cancel a job running at a fork host.

    :param str config: path to arc.conf
    :param str grami_file: path to grami file
    :return: ``True`` if successfully cancelled, else ``False``
    :rtype: :py:obj:`bool`
    """

    debug('----- starting forkCancel.py -----', 'fork.Cancel')

    configure(config)

    grami = SimpleGramiParser(grami_file)
    ctrdir = grami.controldir
    jobid = grami.jobid
    gridid = grami.gridid
    jobdir = get_job_directory(ctrdir, gridid)

    killed = False
    info('Deleting job %s, local id %s' % (gridid, jobid), 'fork.Cancel')
    with open('%s/job.%s.status' % (jobdir, gridid)) as f:
        line = f.readline()
        if re.search('INLRMS|CANCELING'):
            if not Config.remote_host:
                import signal
                os.kill(jobid, signal.SIGTERM)
                time.sleep(5)
                os.kill(jobid, signal.SIGKILL)
                killed = True
            else:
                args = 'kill -s TERM %i; sleep 5; kill -s KILL %i' % (jobid, jobid)
                handle = execute_remote(args)
                if handle.returncode:
                    error('Failed to kill job %i at remote host %s' % (jobid, Config.remote_host), 'fork.Cancel')
                else:
                    killed = True
        elif re.search('FINISHED|DELETED'):
            info('Job already died, won\'t do anything', 'fork.Cancel')
        else:
            info('Job is at unkillable state', 'fork.Cancel')

    debug('----- exiting forkCancel.py -----', 'fork.Cancel')
    return killed


#---------------------
# Scan methods
#---------------------

def Scan(config, ctr_dirs):
    """
    Query the fork host for all jobs in /[controldir]/processing.
    If the job has stopped running, the exit code is read and the 
    diagnostics and comments files are updated.

    :param str config: path to arc.conf
    :param ctr_dirs: list of paths to control directories 
    :type ctr_dirs: :py:obj:`list` [ :py:obj:`str` ... ]
    """

    time.sleep(10)

    configure(config, set_fork)

    if Config.scanscriptlog:
        scanlogfile = arc.common.LogFile(Config.scanscriptlog)
        arc.common.Logger_getRootLogger().addDestination(scanlogfile)

    jobs = get_jobs(ctr_dirs)
    if not jobs: return

    execute = excute_local if not Config.remote_host else execute_remote
    args = 'ps -opid ' + (' '.join(jobs.keys()))
    if os.environ.has_key('__FORK_TEST'):
        handle = execute(args, env=dict(os.environ))
    else:
        handle = execute(args)
    if handle.returncode != 0:
        debug('Got error code %i from ps -opid' % handle.returncode, 'fork.Scan')
        #debug('Output is:', 'fork.Scan')
        #debug('\n'.join(handle.stdout), 'fork.Scan')
        debug('Error output is:', 'fork.Scan')
        debug('\n'.join(handle.stderr), 'fork.Scan')

    running = [line.strip() for line in handle.stdout]
    for localid, job in jobs.items():
        if localid in running:
            continue
        if set_exit_code_from_diag(job):
            grami = SimpleGramiParser(job.grami_file)
            if job.exitcode != grami.arg_code:
                job.message = 'Job finished with wrong exit code - %s != %s' % (job.exitcode, grami.arg_code)
            else:
                job.message = MESSAGES[job.state]
        else:
            job.exitcode = -1
        
        with open(job.lrms_done_file, 'w') as f:
            f.write('%i %s\n' % (job.exitcode, job.message))
        write_comments(job)
